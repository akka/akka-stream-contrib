/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.contrib

import akka.stream.{ Attributes, FanOutShape2 }
import akka.stream.stage.{ GraphStage, GraphStageLogic, OutHandler }
import akka.util.Timeout

/** INTERNAL API */
private[akka] abstract class UnfoldFlowGraphStageLogic[O, S, E] private[stream] (shape: FanOutShape2[O, S, E], seed: S, timeout: Timeout) extends GraphStageLogic(shape) {

  val feedback = shape.out0
  val output = shape.out1
  val nextElem = shape.in

  var pending: S = seed
  var pushedToCycle = false

  setHandler(feedback, new OutHandler {
    override def onPull() = if (!pushedToCycle && isAvailable(output)) {
      push(feedback, pending)
      pending = null.asInstanceOf[S]
      pushedToCycle = true
    }

    override def onDownstreamFinish() = {
      // Do Nothing until `timeout` to try and intercept completion as downstream,
      // but cancel stream after timeout if inlet is not closed to prevent deadlock.
      materializer.scheduleOnce(timeout.duration, new Runnable {
        override def run() = {
          getAsyncCallback[Unit] { _ =>
            if (!isClosed(nextElem)) {
              failStage(new IllegalStateException(s"unfoldFlow source's inner flow canceled only upstream, while downstream remain available for $timeout"))
            }
          }.invoke(())
        }
      })
    }
  })

  setHandler(output, new OutHandler {
    override def onPull() = {
      pull(nextElem)
      if (!pushedToCycle && isAvailable(feedback)) {
        push(feedback, pending)
        pending = null.asInstanceOf[S]
        pushedToCycle = true
      }
    }
  })
}

/** INTERNAL API */
private[akka] class FanOut2unfoldingStage[O, S, E] private[stream] (generateGraphStageLogic: FanOutShape2[O, S, E] => UnfoldFlowGraphStageLogic[O, S, E]) extends GraphStage[FanOutShape2[O, S, E]] {
  override val shape = new FanOutShape2[O, S, E]("unfoldFlow")
  override def createLogic(attributes: Attributes) = generateGraphStageLogic(shape)
}
