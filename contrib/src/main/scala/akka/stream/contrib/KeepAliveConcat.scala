/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.contrib

import akka.stream._
import akka.stream.stage._

import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration

/**
 * Sends elements from buffer if upstream does not emit for a configured amount of time. In other words, this
 * stage attempts to maintains a base rate of emitted elements towards the downstream using elements from upstream.
 *
 * If upstream emits new elements until the accumulated elements in the buffer exceed the specified minimum size
 * used as the keep alive elements, then the base rate is no longer maintained until we reach another period without
 * elements form upstream.
 *
 * The keep alive period is the keep alive failover size times the interval.
 *
 * '''Emits when''' upstream emits an element or if the upstream was idle for the configured period
 *
 * '''Backpressures when''' downstream backpressures
 *
 * '''Completes when''' upstream completes
 *
 * '''Cancels when''' downstream cancels
 *
 * @see [[akka.stream.scaladsl.FlowOps#keepAlive]]
 * @see [[akka.stream.scaladsl.FlowOps#expand]]
 */
final case class KeepAliveConcat[T](keepAliveFailoverSize: Int, interval: FiniteDuration, extrapolate: T ⇒ Seq[T])
  extends GraphStage[FlowShape[T, T]] {

  require(keepAliveFailoverSize > 0, "The buffer keep alive failover size must be greater than 0.")

  val in = Inlet[T]("KeepAliveConcat.in")
  val out = Outlet[T]("KeepAliveConcat.out")

  override val shape = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new TimerGraphStageLogic(shape) with InHandler with OutHandler {

      private val buffer = new java.util.ArrayDeque[T](keepAliveFailoverSize)

      override def preStart(): Unit = {
        schedulePeriodically(None, interval)
        pull(in)
      }

      override def onPush(): Unit = {
        val elem = grab(in)
        if (buffer.size() < keepAliveFailoverSize) buffer.addAll(extrapolate(elem).asJava)
        else buffer.addLast(elem)

        if (isAvailable(out) && !buffer.isEmpty) push(out, buffer.removeFirst())
        else pull(in)
      }

      override def onPull(): Unit = {
        if (isClosed(in)) {
          if (buffer.isEmpty) completeStage()
          else push(out, buffer.removeFirst())
        } else if (buffer.size() > keepAliveFailoverSize) {
          push(out, buffer.removeFirst())
        } else if (!hasBeenPulled(in)) {
          pull(in)
        }
      }

      override def onTimer(timerKey: Any) = {
        if (isAvailable(out) && !buffer.isEmpty) push(out, buffer.removeFirst())
      }

      override def onUpstreamFinish(): Unit = {
        if (buffer.isEmpty) completeStage()
      }

      setHandlers(in, out, this)
    }
}
