/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.contrib

import akka.japi.function
import akka.stream.stage.{GraphStage, InHandler, OutHandler, TimerGraphStageLogic}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}

import scala.collection.immutable
import scala.concurrent.duration.FiniteDuration

object AccumulateWhileUnchanged {

  /**
   * Factory for [[AccumulateWhileUnchanged]] instances
   *
   * @param propertyExtractor a function to extract the observed element property
   * @param maxElements maximum number of elements to accumulate before emitting, if defined.
   * @param maxDuration maximum duration to accumulate elements before emitting, if defined.
   * @tparam Element  type of accumulated elements
   * @tparam Property type of the observed property
   * @return [[AccumulateWhileUnchanged]] instance
   */
  def apply[Element, Property](propertyExtractor: Element => Property,
                               maxElements: Option[Int] = None,
                               maxDuration: Option[FiniteDuration] = None) =
    new AccumulateWhileUnchanged(propertyExtractor, maxElements, maxDuration)

  /**
   * Java API: Factory for [[AccumulateWhileUnchanged]] instances
   *
   * @param propertyExtractor a function to extract the observed element property
   * @param maxElements maximum number of elements to accumulate before emitting, if defined.
   * @param maxDuration maximum duration to accumulate elements before emitting, if defined.
   * @tparam Element  type of accumulated elements
   * @tparam Property type of the observed property
   * @return [[AccumulateWhileUnchanged]] instance
   */
  def create[Element, Property](propertyExtractor: function.Function[Element, Property],
                                maxElements: Option[Int] = None,
                                maxDuration: Option[FiniteDuration] = None) =
    new AccumulateWhileUnchanged(propertyExtractor.apply, maxElements, maxDuration)
}

/**
 * Accumulates elements of type [[Element]] while a property extracted with [[propertyExtractor]] remains unchanged,
 * emits an accumulated sequence when the property changes, maxElements is reached or maxDuration has passed.
 *
 * @param propertyExtractor a function to extract the observed element property
 * @param maxElements maximum number of elements to accumulate before emitting, if defined.
 * @param maxDuration maximum duration to accumulate elements before emitting, if defined.
 * @tparam Element  type of accumulated elements
 * @tparam Property type of the observed property
 */
final class AccumulateWhileUnchanged[Element, Property](propertyExtractor: Element => Property,
                                                        maxElements: Option[Int] = None,
                                                        maxDuration: Option[FiniteDuration] = None)
    extends GraphStage[FlowShape[Element, immutable.Seq[Element]]] {

  val in = Inlet[Element]("AccumulateWhileUnchanged.in")
  val out = Outlet[immutable.Seq[Element]]("AccumulateWhileUnchanged.out")

  override def shape = FlowShape.of(in, out)

  override def createLogic(inheritedAttributes: Attributes) = new TimerGraphStageLogic(shape) {

    private var currentState: Option[Property] = None
    private var nbElements: Int = 0
    private var downstreamWaiting = false
    private val buffer = Vector.newBuilder[Element]

    setHandlers(
      in,
      out,
      new InHandler with OutHandler {

        override def onPush(): Unit = {
          val nextElement = grab(in)
          val nextState = propertyExtractor(nextElement)

          if (currentState.isEmpty) currentState = Some(nextState)

          (currentState, maxElements) match {
            case (Some(`nextState`), None) => stash(nextElement)
            case (Some(`nextState`), Some(max)) if nbElements < max => stash(nextElement)
            case _ => pushResults(Some(nextElement), Some(nextState))
          }
        }

        override def onPull(): Unit = {
          downstreamWaiting = true
          if (!hasBeenPulled(in)) {
            pull(in)
          }
        }

        override def onUpstreamFinish(): Unit = {
          val result = buffer.result()
          if (result.nonEmpty) {
            emit(out, result)
          }
          completeStage()
        }

        private def stash(nextElement: Element) = {
          buffer += nextElement
          nbElements += 1
          if (downstreamWaiting) pull(in)
        }
      }
    )

    override def preStart(): Unit = {
      super.preStart()
      maxDuration match {
        case Some(max) => schedulePeriodically(None, max)
        case None => ()
      }
    }
    override def postStop(): Unit =
      buffer.clear()

    override protected def onTimer(timerKey: Any): Unit =
      pushResults(None, None)

    private def pushResults(nextElement: Option[Element], nextState: Option[Property]): Unit = {
      if (!isAvailable(out)) {
        require(nextElement.isEmpty, s"pushResults: not available, would drop nextElement=$nextElement")
        return
      }

      val result = buffer.result()
      buffer.clear()
      nbElements = 0

      if (result.nonEmpty) {
        push(out, result)
        downstreamWaiting = false
      }

      nextElement match {
        case Some(next) =>
          buffer += next
          nbElements += 1
        case None => ()
      }

      currentState = nextState
    }
  }
}
