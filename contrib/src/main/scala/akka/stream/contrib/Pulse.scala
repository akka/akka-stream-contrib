/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.contrib

import akka.stream.Attributes
import akka.stream.impl.fusing.GraphStages.SimpleLinearGraphStage
import akka.stream.stage._

import scala.concurrent.duration.FiniteDuration

/**
 * Signals demand only once every [[interval]] (''pulse'') and then back-pressures. Requested element is emitted downstream if there is demand.
 *
 * It can be used to implement simple time-window processing
 * where data is aggregated for predefined amount of time and the computed aggregate is emitted once per this time.
 * See [[TimeWindow]]
 *
 * @param interval ''pulse'' period
 * @param initiallyOpen if `true` - emits the first available element before ''pulsing''
 * @tparam T type of element
 */
final class Pulse[T](val interval: FiniteDuration, val initiallyOpen: Boolean = false) extends SimpleLinearGraphStage[T] {
  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new TimerGraphStageLogic(shape) with InHandler with OutHandler {

      setHandlers(in, out, this)

      override def preStart(): Unit = if (!initiallyOpen) startPulsing()
      override def onPush(): Unit = if (isAvailable(out)) push(out, grab(in))
      override def onPull(): Unit = if (!pulsing) {
        pull(in)
        startPulsing()
      }

      override protected def onTimer(timerKey: Any): Unit = {
        if (isAvailable(out) && !isClosed(in) && !hasBeenPulled(in)) pull(in)
      }

      private def startPulsing() = {
        pulsing = true
        schedulePeriodically("PulseTimer", interval)
      }
      private var pulsing = false
    }

  override def toString = "Pulse"
}
