/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.contrib

import akka.stream._
import akka.stream.scaladsl._
import akka.stream.stage._
import akka.util.Timeout

/**
 * Source factory methods are placed here
 */
object SourceGen {
  /**
   * Create a `Source` that will unfold a value of type `S` by
   * passing it through a flow. The flow should emit a
   * pair of the next state `S` and output elements of type `E`.
   * Source completes when the flow completes.
   *
   * The `timeout` parameter specifies waiting time after inner
   * flow provided by the user for unfold flow API cancels
   * upstream, to get also the downstream cancelation (as
   * graceful completion or failure which is propagated).
   * If inner flow fails to complete/fail downstream, stage is
   * failed with an IllegalStateException.
   *
   * IMPORTANT CAVEAT:
   * The given flow must not change the number of elements passing through it (i.e. it should output
   * exactly one element for every received element). Ignoring this, will have an unpredicted result,
   * and may result in a deadlock.
   */
  def unfoldFlow[S, E, M](seed: S)(flow: Graph[FlowShape[S, (S, E)], M])(implicit timeout: Timeout): Source[E, M] = {

    val generateUnfoldFlowGraphStageLogic = (shape: FanOutShape2[(S, E), S, E]) => new UnfoldFlowGraphStageLogic[(S, E), S, E](shape, seed, timeout) {
      setHandler(nextElem, new InHandler {
        override def onPush() = {
          val (s, e) = grab(nextElem)
          pending = s
          push(output, e)
          pushedToCycle = false
        }
      })
    }

    unfoldFlowGraph(new FanOut2unfoldingStage(generateUnfoldFlowGraphStageLogic), flow)
  }

  /**
   * Create a `Source` that will unfold a value of type `S` by
   * passing it through a flow. The flow should emit an output
   * value of type `O`, that when fed to the unfolding function,
   * generates a pair of the next state `S` and output elements of type `E`.
   *
   * The `timeout` parameter specifies waiting time after inner
   * flow provided by the user for unfold flow API cancels
   * upstream, to get also the downstream cancelation (as
   * graceful completion or failure which is propagated).
   * If inner flow fails to complete/fail downstream, stage is
   * failed with an IllegalStateException.
   *
   * IMPORTANT CAVEAT:
   * The given flow must not change the number of elements passing through it (i.e. it should output
   * exactly one element for every received element). Ignoring this, will have an unpredicted result,
   * and may result in a deadlock.
   */
  def unfoldFlowWith[E, S, O, M](seed: S, flow: Graph[FlowShape[S, O], M])(unfoldWith: O => Option[(S, E)])(implicit timeout: Timeout): Source[E, M] = {

    val generateUnfoldFlowGraphStageLogic = (shape: FanOutShape2[O, S, E]) => new UnfoldFlowGraphStageLogic[O, S, E](shape, seed, timeout) {
      setHandler(nextElem, new InHandler {
        override def onPush() = {
          val o = grab(nextElem)
          unfoldWith(o) match {
            case None => completeStage()
            case Some((s, e)) => {
              pending = s
              push(output, e)
              pushedToCycle = false
            }
          }
        }
      })
    }

    unfoldFlowGraph(new FanOut2unfoldingStage(generateUnfoldFlowGraphStageLogic), flow)
  }

  /** INTERNAL API */
  private[akka] def unfoldFlowGraph[E, S, O, M](
    fanOut2Stage: GraphStage[FanOutShape2[O, S, E]],
    flow:         Graph[FlowShape[S, O], M]
  ): Source[E, M] = Source.fromGraph(GraphDSL.create(flow) {
    implicit b =>
      {
        f =>
          {
            import GraphDSL.Implicits._

            val fo2 = b.add(fanOut2Stage)
            fo2.out0 ~> f ~> fo2.in
            SourceShape(fo2.out1)
          }
      }
  })
}
