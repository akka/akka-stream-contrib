/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.contrib

import akka.stream.impl.Stages.DefaultAttributes
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, Inlet, Outlet, UniformFanInShape}

import scala.collection.{immutable, mutable}

/**
 * Merges multiple incoming inputs based on a total ordering extracted from the elements.
 *
 * Merging is done by keeping track of an expected next index value (starting with zero and monotonically increasing).
 * The merging is able to cope with gaps in the index sequence that emerge from filtering out elements before merging.
 * This stage buffers up to <code>inputPorts</code> elements before emitting.
 *
 * '''Emits when''' one input element <em>e</em> consumed has <em>index(e)</em> being the next expected index, or when
 * all input ports have provided elements; in this case, an index omission is assumed, and the element with the lowest
 * index is emitted
 *
 * '''Backpressures when''' downstream backpressures, and one element from each input port has been buffered
 *
 * '''Completes when''' all upstreams complete and all buffered elements were emitted
 *
 * '''Cancels when''' downstream cancels
 *
 * '''Errors when''' the index sequence isn't strict monotonically increasing (e.g. if it contains duplicate index values)
 */
object MergeByIndex {

  /**
   * Creates a merge-by-index stage for tuples with second component of type Long (e.g. as created by <code>.zipWithIndex</code>)
   *
   * @param inputPorts number of inputs to merge
   * @tparam T type of the first tuple component
   * @return a merge stage that orders tuples by their second component.
   */
  def apply[T](inputPorts: Int): MergeByIndex[(T, Long)] = new MergeByIndex[(T, Long)](inputPorts, _._2)

  /**
   * Creates a merge-by-index stage where the index of elements is determined by the <code>index</code> function.
   *
   * @param inputPorts the number of inputs to merge
   * @param index extractor function yielding the index of an element
   * @tparam T type of elements to merge
   * @return a merge stage that orders tuples by their index as specified by the index extractor function.
   */
  def apply[T](inputPorts: Int, index: T => Long): MergeByIndex[T] = new MergeByIndex[T](inputPorts, index)
}

/**
 * A merge stage that respects element ordering as given by their index.
 *
 * @param inputPorts number of inputs to merge
 * @param index extractor function yielding the index of an element
 * @tparam T type of elements to merge
 */
final class MergeByIndex[T](val inputPorts: Int, index: T => Long) extends GraphStage[UniformFanInShape[T, T]] {

  // one input might seem counter intuitive but saves us from special handling in other places
  require(inputPorts >= 1, "A MergeByIndex must have one or more input ports")

  val in: immutable.IndexedSeq[Inlet[T]] = Vector.tabulate(inputPorts)(i => Inlet[T]("MergeByIndex.in" + i))
  val out: Outlet[T] = Outlet[T]("MergeByIndex.out")

  override def initialAttributes: Attributes = DefaultAttributes.merge
  override val shape: UniformFanInShape[T, T] = UniformFanInShape(out, in: _*)

  private type QueueT = (T, Long, Int)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {

    // keeps track of the next expected index
    private var expectedIndex: Long = 0L

    // buffers pulled elements in order of their index
    private val buffer = mutable.PriorityQueue.empty(Ordering.by((i: QueueT) => i._2).reverse)

    // keeps track of closed inlets that have elements in the buffer - relevant for correctly handling index omissions
    // when upstreams start to complete
    private val bufferedClosedInlets = mutable.BitSet.empty

    // keeps track of the maximum expected buffer length - relevant for handling index omissions
    private var maxBufferLength = inputPorts

    override def preStart(): Unit = {
      var ix = 0
      while (ix < in.size) {
        tryPull(in(ix))
        ix += 1
      }
    }

    private def maybeEmit(): Unit = {
      if (buffer.nonEmpty) {
        if (buffer.head._2 == expectedIndex) {
          emitAndPull(buffer.dequeue())
        } else if (elementsFromAllInletsBuffered) {
          // if all inlets pushed elements and we didn't find the expected index, we know this is an index omission.
          // it is therefore fine and necessary to emit the element with the smallest index seen.
          emitAndPull(buffer.dequeue())
        }
      }

      if (noMoreElementsExpected) completeStage()
    }

    private def elementsFromAllInletsBuffered = buffer.length == maxBufferLength

    private def updateMaxBufferLength(): Unit =
      // needs to account for open inlets and elements of closed inlets that is already buffered.
      // only if this amount of elements are in the buffer, we have received an element from each upstream and it is safe
      // to deduce an index omission.
      maxBufferLength = in.count(i => !isClosed(i)) + bufferedClosedInlets.size

    private def noMoreElementsExpected = maxBufferLength == 0

    private def emitAndPull(queueElem: QueueT): Unit = {
      val (elem, index, inletIndex) = queueElem
      val inlet = in(inletIndex)
      verifyElementIndex(index, inlet)
      push(out, elem)
      if (!isClosed(inlet)) {
        pull(inlet)
      } else {
        // Optimization: check this only if inlet is closed.
        if (bufferedClosedInlets.contains(inletIndex)) {
          // in case this inlet was closed and an element was buffered, it isn't any more now.
          bufferedClosedInlets.remove(inletIndex)
          updateMaxBufferLength()
        }
      }
      expectedIndex = index + 1
    }

    private def verifyElementIndex(elemIndex: Long, in: Inlet[T]): Unit =
      if (elemIndex < expectedIndex)
        throw new IllegalArgumentException(
          s"Index sequence is non-monotonic: element received from ${in.s} with index $elemIndex has smaller than currently expected index $expectedIndex"
        )

    {
      var ix = 0
      while (ix < in.size) {
        val i = in(ix)
        setHandler(i, createInHandler(ix, i))
        ix += 1
      }
    }

    private def createInHandler(pos: Int, in: Inlet[T]) = new InHandler {
      override def onPush(): Unit = {
        val elem = grab(in)
        val elemIndex = index(elem)
        verifyElementIndex(elemIndex, in)
        buffer.enqueue((elem, elemIndex, pos))
        if (isAvailable(out)) maybeEmit()
      }

      override def onUpstreamFinish(): Unit = {
        // for properly handling index omissions we need to remember how many elements from closed inlets are buffered.
        if (buffer.exists(_._3 == pos)) bufferedClosedInlets.add(pos)
        updateMaxBufferLength()
        if (isAvailable(out)) maybeEmit() // a finished upstream may unblock emitting.
        else if (noMoreElementsExpected) completeStage()
      }
    }

    setHandler(out, new OutHandler {
      override def onPull(): Unit = maybeEmit()
    })
  }

  override def toString = "MergeByIndex"
}
