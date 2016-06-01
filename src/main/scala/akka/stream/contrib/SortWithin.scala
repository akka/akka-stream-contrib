/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.contrib

import akka.stream.Attributes
import akka.stream.impl.fusing.GraphStages.SimpleLinearGraphStage
import akka.stream.stage.{GraphStageLogic, InHandler, OutHandler, TimerGraphStageLogic}

import scala.collection.immutable
import scala.concurrent.duration.FiniteDuration

object SortWithin {

  /**
    * Factory for [[SortWithin]] instances.
    *
    * @param finiteDuration duration within which sorting needs to be done. Ideally should be kept as small as possible
    * @tparam A input type
    * @return [[SortWithin]] instance.
    */
  def apply[A <: Comparable[A]](finiteDuration: FiniteDuration): SortWithin[A] = new SortWithin[A](finiteDuration)

  /**
    * Java API: Factory for [[SortWithin]] instances.
    *
    * @param finiteDuration duration within which sorting needs to be done. Ideally should be kept as small as possible
    * @tparam A input type
    * @return [[SortWithin]] instance.
    */
  def create[A <: Comparable[A]](finiteDuration: FiniteDuration): SortWithin[A] = new SortWithin[A](finiteDuration)
}

final class SortWithin[T <: Comparable[T]](timeout: FiniteDuration) extends SimpleLinearGraphStage[T] {

  var sortedElements = immutable.TreeSet[T]()

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new TimerGraphStageLogic(shape) {
    setHandler(in, new InHandler {
      override def onPush(): Unit = {
        val element: T = grab(in)
        sortedElements += element
        pull(in)
      }

      @scala.throws[Exception](classOf[Exception])
      override def onUpstreamFinish(): Unit = {
        emitMultiple(out, sortedElements.iterator)
        super.onUpstreamFinish()
      }

      @scala.throws[Exception](classOf[Exception])
      override def onUpstreamFailure(ex: Throwable): Unit = {
        emitMultiple(out, sortedElements.iterator)
        super.onUpstreamFailure(ex)
      }
    })

    setHandler(out, new OutHandler {
      override def onPull(): Unit = pull(in)
    })


    final override protected def onTimer(key: Any): Unit = {
      emitMultiple(out, sortedElements.iterator)
      completeStage()
    }

    override def preStart(): Unit = scheduleOnce("SortWithinTimer", timeout)
  }

  override def toString = "SortWithin"
}