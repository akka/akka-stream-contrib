/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.contrib

import akka.stream.scaladsl.GraphDSL
import akka.stream.{Attributes, FanInShape2, FlowShape, Graph, Inlet, Outlet, SourceShape}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.util.OptionVal
import akka.util.OptionVal._

/**
 * Throttles a flow based on tokens provided consumed from a provided token source. The flow emits elements only if
 * the needed amount of tokens for the next element is available. The amount of tokens needed is specified by a cost
 * function.
 *
 * Tokens are consumed as-needed, and pre-fetched once the internal token buffer is empty. Also, an initial fetch is
 * performed to initially fill the internal token bucket.
 *
 * '''Emits when''' when an input element <em>e</em> is available and <em>costCalculation(e)</em> tokens are available.
 *
 * '''Backpressures when''' downstream backpressures, or when not enough tokens are available for the next element. For
 * the token input: when no tokens are needed (tokens are pulled as needed).
 *
 * '''Completes when''' a) element upstream completes, or b) when token upstream completes and all internally stored
 * tokens were consumed (best effort; also completes when the next element cost is higher than the available tokens).
 *
 * '''Cancels when''' downstream cancels.
 */
object TokenThrottle {

  /**
   * Creates a token-based throttling flow.
   *
   * @param tokenSource source of tokens to use for controlling throughput.
   * @param costCalculation cost function that determines the cost of a given element
   * @tparam A element type
   * @tparam M materialized value of token source
   * @return simple token throttle graph.
   */
  def apply[A, M](tokenSource: Graph[SourceShape[Long], M])(costCalculation: A => Long): Graph[FlowShape[A, A], M] =
    GraphDSL.create(tokenSource) { implicit b => tokens =>
      import GraphDSL.Implicits._
      val throttle = b.add(new TokenThrottle[A](costCalculation))
      tokens ~> throttle.in1
      FlowShape(throttle.in0, throttle.out)
    }
}

/**
 * Graph stage for token-based throttling. Use it directly for constructing more complex graphs.
 *
 * @param costCalculation cost function that determines the cost of a given element
 * @tparam A element type
 */
final class TokenThrottle[A](costCalculation: A => Long) extends GraphStage[FanInShape2[A, Long, A]] {
  override def initialAttributes: Attributes = Attributes.name("TokenThrottle")

  override val shape: FanInShape2[A, Long, A] = new FanInShape2[A, Long, A]("TokenThrottle")

  def out: Outlet[A] = shape.out

  val elemsIn: Inlet[A] = shape.in0
  val tokensIn: Inlet[Long] = shape.in1

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {

    var tokens: Long = 0

    var buffer: OptionVal[A] = none

    var cost: Long = -1 // invariant: buffer.isDefined => cost == costCalculation(buffer.get)

    var tokensCompleted = false

    var elemsCompleted = false

    private def maybeEmit(): Unit =
      if (isAvailable(out) && buffer.isDefined) {
        if (tokens >= cost) {
          tokens -= cost
          push(out, buffer.get)
          buffer = none
          if (elemsCompleted || tokensExhausted) {
            completeStage()
          } else {
            pull(elemsIn)
            if (tokens == 0) askForTokens()
          }
        } else {
          if (!tokensCompleted) askForTokens() else completeStage()
        }
      }

    private def tokensExhausted = tokensCompleted && tokens == 0

    private def askForTokens(): Unit = if (!tokensCompleted && !hasBeenPulled(tokensIn)) pull(tokensIn)

    override def preStart(): Unit = {
      pull(elemsIn)
      pull(tokensIn)
    }

    setHandler(
      elemsIn,
      new InHandler {
        override def onPush(): Unit = {
          val elem = grab(elemsIn)
          buffer = Some(elem)
          cost = costCalculation(elem) // pre-compute cost here to call cost function only once per element
          if (cost < 0) throw new IllegalArgumentException("Cost must be non-negative")
          maybeEmit()
        }

        override def onUpstreamFinish(): Unit = {
          if (buffer.isEmpty) completeStage()
          elemsCompleted = true
        }
      }
    )

    setHandler(
      tokensIn,
      new InHandler {
        override def onPush(): Unit = {
          tokens += grab(tokensIn)
          maybeEmit()
        }

        override def onUpstreamFinish(): Unit = {
          if (tokens == 0) completeStage()
          tokensCompleted = true
        }
      }
    )

    setHandler(out, new OutHandler {
      override def onPull(): Unit = maybeEmit()
    })
  }
}
