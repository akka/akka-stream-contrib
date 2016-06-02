/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.contrib

import akka.stream.Attributes
import akka.stream.impl.fusing.GraphStages.SimpleLinearGraphStage
import akka.stream.stage.{GraphStageLogic, InHandler, OutHandler, TimerGraphStageLogic}

import scala.collection.immutable.VectorBuilder
import scala.concurrent.duration.{Duration, FiniteDuration}

object SortWithin {

  /**
    * Sorts elements received within given duration and emits them at the end of each duration.
    *
    * '''Emits when''' at regular interval of finite duration timeout
    *
    * '''Backpressures when''' downstream backpressures
    *
    * '''Completes when''' upstream completes
    *
    * '''Cancels when''' downstream cancels
    *
    * @param finiteDuration duration within which sorting needs to be done. Ideally should be kept as small as possible
    * @tparam A input type
    * @return [[SortWithin]] instance.
    */
  def apply[A <: Comparable[A]](finiteDuration: FiniteDuration): SortWithin[A] = new SortWithin[A](finiteDuration)

  /**
    * Java API: Sorts elements received within given duration and emits them at the end of each duration.
    *
    * '''Emits when''' at regular interval of finite duration timeout
    *
    * '''Backpressures when''' downstream backpressures
    *
    * '''Completes when''' upstream completes
    *
    * '''Cancels when''' downstream cancels
    *
    * @param finiteDuration duration within which sorting needs to be done. Ideally should be kept as small as possible
    * @tparam A input type
    * @return [[SortWithin]] instance.
    */
  def create[A <: Comparable[A]](finiteDuration: FiniteDuration): SortWithin[A] = new SortWithin[A](finiteDuration)
}

final class SortWithin[T <: Comparable[T]](timeout: FiniteDuration) extends SimpleLinearGraphStage[T] {
  require(timeout > Duration.Zero)

  private val buf: VectorBuilder[T] = new VectorBuilder

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new TimerGraphStageLogic(shape) {
    setHandler(in, new InHandler {
      override def onPush(): Unit = {
        buf += grab(in)
        pull(in)
      }

      override def onUpstreamFinish(): Unit = {
        emitMultiple(out, buf.result().sorted)
        buf.clear()
        super.onUpstreamFinish()
      }

      override def onUpstreamFailure(ex: Throwable): Unit = {
        emitMultiple(out, buf.result().sorted)
        buf.clear()
        super.onUpstreamFailure(ex)
      }
    })

    setHandler(out, new OutHandler {
      override def onPull(): Unit = {}
    })


    final override protected def onTimer(key: Any): Unit = {
      emitMultiple(out, buf.result().sorted)
      buf.clear()
    }

    override def preStart(): Unit = {
      schedulePeriodically("SortWithinTimer", timeout)
      pull(in)
    }
  }

  override def toString = "SortWithin"
}