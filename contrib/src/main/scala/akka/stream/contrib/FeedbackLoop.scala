/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.contrib

import akka.stream.scaladsl.{ Flow, GraphDSL, Keep, MergePreferred }
import akka.stream.stage.{ GraphStage, GraphStageLogic, InHandler, OutHandler }
import akka.stream.{ Attributes, FanOutShape2, FlowShape, Graph, Inlet, Outlet, OverflowStrategy }

object FeedbackLoop {

  /**
   * Feeds `forwardFlow`'s first output [[O0]] via `feedbackArc` back to `forwardFlow`'s input.
   * To prevent deadlocks, `feedbackArc` is never backpressured. Instead, elements emitted by
   * the `feedbackArc` are buffered and the resulting flow fails if that buffer overflows.
   */
  def apply[I, O0, O, M1, M2, M](
    forwardFlow:        Graph[FanOutShape2[I, O0, O], M1],
    feedbackArc:        Graph[FlowShape[O0, I], M2],
    feedbackBufferSize: Int)(
    combineMat: (M1, M2) => M): Flow[I, O, M] = Flow.fromGraph(GraphDSL.create(forwardFlow, feedbackArc)(combineMat) { implicit builder => (fw, fb) => {
    import GraphDSL.Implicits._

    val merge = builder.add(MergePreferred[I](1))
    merge.out ~> fw.in

    // Feed forwardFlow's first output to feedbackArc, but do not signal feedbackArc's cancellation to forwardFlow.
    // Failure or completion will propagate to forwardFlow from the other end of the feedbackArc.
    fb <~ ignoreAfterDownstreamFinish[O0] <~ fw.out0

    // Feed feedbackArc back to forwardFlow, but never backpressure feedbackArc.
    // To that end, use an intermediate buffer that fails on overflow.
    merge.preferred <~ Flow[I].buffer(feedbackBufferSize, OverflowStrategy.fail) <~ fb

    FlowShape(merge.in(0), fw.out1)
  }
  })

  object Implicits {
    implicit class FanOut2Ops[I, O0, O, M1](val fanOut: Graph[FanOutShape2[I, O0, O], M1]) extends AnyVal {
      def feedbackViaMat[M2, M](feedbackArc: Graph[FlowShape[O0, I], M2], feedbackBufferSize: Int)(combineMat: (M1, M2) => M): Flow[I, O, M] =
        FeedbackLoop(fanOut, feedbackArc, feedbackBufferSize)(combineMat)

      def feedbackVia[M2](feedbackArc: Graph[FlowShape[O0, I], M2], feedbackBufferSize: Int): Flow[I, O, M1] =
        feedbackViaMat(feedbackArc, feedbackBufferSize)(Keep.left)
    }

    implicit class FeedbackShapeOps[I, O, M](val fanOut: Graph[FanOutShape2[I, I, O], M]) extends AnyVal {
      def feedback(feedbackBufferSize: Int): Flow[I, O, M] =
        fanOut.feedbackVia(Flow[I], feedbackBufferSize)
    }
  }

  /**
   * Flow that passes all upstream elements downstream and after downstream completes,
   * keeps consuming (and ignoring) all upstream elements.
   */
  private def ignoreAfterDownstreamFinish[T]: GraphStage[FlowShape[T, T]] =
    new GraphStage[FlowShape[T, T]] {
      override val shape: FlowShape[T, T] = FlowShape(Inlet("in"), Outlet("out"))

      override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
        var downstreamFinished = false

        setHandler(shape.out, new OutHandler {
          override def onPull(): Unit = pull(shape.in)

          override def onDownstreamFinish(): Unit = {
            downstreamFinished = true
            if (!hasBeenPulled(shape.in)) {
              pull(shape.in)
            }
          }
        })

        setHandler(shape.in, new InHandler {
          def onPush(): Unit = {
            val elem = grab(shape.in)
            if (downstreamFinished) {
              pull(shape.in)
            } else {
              push(shape.out, elem)
            }
          }
        })
      }
    }
}
