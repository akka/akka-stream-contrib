/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.contrib

import akka.stream.Attributes.name
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, Inlet, Outlet, UniformFanOutShape}

import scala.collection.mutable

object BufferedBroadcast {

  def apply[T](outputs: Int) = new BufferedBroadcast[T](outputs)
}

/**
 * Broadcasta elements to many downstreams, but buffers all the elements for downstreams without demand,
 * if there is some downstream with demand. If downstream with buffered elements signals demand,
 * the buffered elements are sent.
 *
 * * '''Emits when''' upstream emits an element
 * *
 * * '''Backpressures when''' all downstreams backpressure
 * *
 * * '''Completes when''' upstream completes and all buffered elements are sent to downstreams
 * *
 * * '''Cancels when''' all downstreams cancel
 *
 * @param outputs number of outputs
 * @tparam T type of element
 */
class BufferedBroadcast[T](outputs: Int) extends GraphStage[UniformFanOutShape[T, T]] {

  case class BufferedOutlet(
      out: Outlet[T],
      buffer: mutable.Queue[T] = new mutable.Queue[T](),
      var pending: Boolean = true
  )

  private val in: Inlet[T] = Inlet("bufferedBroadcast.in")
  private val outs = Vector.tabulate(outputs)(i => BufferedOutlet(Outlet[T]("bufferedBroadcast.out" + i)))
  override val initialAttributes: Attributes = name("bufferedBroadcast")
  override val shape: UniformFanOutShape[T, T] = UniformFanOutShape(in, outs.map(_.out): _*)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      private var downstreamsRunning = outputs
      private var upstreamFinished = false

      private def tryComplete(): Unit =
        if (upstreamFinished) {
          outs.foreach { o =>
            if (o.buffer.nonEmpty && o.pending) {
              push(o.out, o.buffer.dequeue())
              o.pending = false
            }

            if (o.buffer.isEmpty) complete(o.out)
          }
        }

      private def tryPull(): Unit = if (!upstreamFinished && !hasBeenPulled(in)) pull(in)

      setHandler(
        in,
        new InHandler {
          def onPush(): Unit = {
            val elem = grab(in)
            outs.foreach { o =>
              if (o.pending) {
                push(o.out, elem)
                o.pending = false
              } else {
                o.buffer.enqueue(elem)
              }
            }
          }

          override def onUpstreamFinish(): Unit = {
            upstreamFinished = true
            tryComplete()
          }
        }
      )

      outs.foreach { o =>
        setHandler(
          o.out,
          new OutHandler {
            override def onPull(): Unit =
              if (o.buffer.nonEmpty) {
                push(o.out, o.buffer.dequeue())
                o.pending = false
                tryComplete()
              } else {
                o.pending = true
                tryPull()
              }

            override def onDownstreamFinish(): Unit = {
              downstreamsRunning -= 1
              o.buffer.clear()
              if (downstreamsRunning == 0) completeStage()
            }
          }
        )
      }
    }

  override def toString = "BufferedBroadcast"
}
