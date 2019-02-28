/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.contrib

import akka.japi.function
import akka.stream.scaladsl.{ GraphDSL, Keep }
import akka.stream.{ Attributes, FanOutShape2, FlowShape, Graph }
import akka.stream.stage.{ GraphStage, GraphStageLogic, InHandler, OutHandler }

/**
 * This companion defines a factory for [[PartitionWith]] instances, see [[PartitionWith.apply]].
 */
object PartitionWith {

  /**
   * Factory for [[PartitionWith]] instances.
   *
   * @param p partition function
   * @param eagerCancel when `false` (the default), cancel after all downstream have cancelled.
   *                    When `true`, cancel as soon as any downstream cancels and complete the remaining downstreams
   * @tparam In input type
   * @tparam Out0 left output type
   * @tparam Out1 right output type
   * @return [[PartitionWith]] instance
   */
  def apply[In, Out0, Out1](p: In => Either[Out0, Out1], eagerCancel: Boolean = false): PartitionWith[In, Out0, Out1] =
    new PartitionWith(p, eagerCancel)

  /**
   * Java API: Factory for [[PartitionWith]] instances.
   *
   * @param p partition function
   * @param eagerCancel when `false` (the default), cancel after all downstream have cancelled.
   *                    When `true`, cancel as soon as any downstream cancels and complete the remaining downstreams
   * @tparam In input type
   * @tparam Out0 left output type
   * @tparam Out1 right output type
   * @return [[PartitionWith]] instance
   */
  def create[In, Out0, Out1](p: function.Function[In, Either[Out0, Out1]], eagerCancel: Boolean = false): PartitionWith[In, Out0, Out1] =
    new PartitionWith(p.apply, eagerCancel)

  object Implicits {
    implicit final class FlowGraphOps[In, Out, M](val flowGraph: Graph[FlowShape[In, Out], M]) extends AnyVal {

      /**
       * Partition the output of the decorated flow according to the given partition function.
       */
      def partitionWith[Out0, Out1](p: Out => Either[Out0, Out1]): Graph[FanOutShape2[In, Out0, Out1], M] = {
        GraphDSL.create(flowGraph, PartitionWith(p))(Keep.left) { implicit builder => (flow, fanOut) => {
          import GraphDSL.Implicits._
          flow.out ~> fanOut.in
          new FanOutShape2(flow.in, fanOut.out0, fanOut.out1)
        }
        }
      }
    }
  }
}

/**
 * This stage partitions input to 2 different outlets,
 * applying different transformations on the elements,
 * according to the received partition function.
 *
 * @param p partition function
 * @param eagerCancel when `false` (the default), cancel after all downstream have cancelled.
 *                    When `true`, cancel as soon as any downstream cancels and complete the remaining downstreams
 * @tparam In input type
 * @tparam Out0 left output type
 * @tparam Out1 right output type
 */
final class PartitionWith[In, Out0, Out1] private (p: In => Either[Out0, Out1], eagerCancel: Boolean)
  extends GraphStage[FanOutShape2[In, Out0, Out1]] {

  override val shape = new FanOutShape2[In, Out0, Out1]("partitionWith")

  override def createLogic(attributes: Attributes) = new GraphStageLogic(shape) {
    import shape._

    private var pending: Either[Out0, Out1] = null
    private var activeDownstreamCount = 2

    setHandler(in, new InHandler {
      override def onPush() = {
        val elem = grab(in)
        p(elem) match {
          case Left(o) if isAvailable(out0) =>
            push(out0, o)
            if (isAvailable(out1))
              pull(in)
          case Right(o) if isAvailable(out1) =>
            push(out1, o)
            if (isAvailable(out0))
              pull(in)
          case either =>
            pending = either
        }
      }

      override def onUpstreamFinish() = {
        if (pending eq null)
          completeStage()
      }
    })

    setHandler(out0, new OutHandler {
      override def onPull() = if (pending ne null) pending.left.foreach { o =>
        push(out0, o)
        if (isClosed(in)) completeStage()
        else {
          pending = null
          if (isAvailable(out1))
            pull(in)
        }
      }
      else if (!hasBeenPulled(in)) pull(in)

      override def onDownstreamFinish(): Unit =
        downstreamFinished()
    })

    setHandler(out1, new OutHandler {
      override def onPull() = if (pending ne null) pending.right.foreach { o =>
        push(out1, o)
        if (isClosed(in)) completeStage()
        else {
          pending = null
          if (isAvailable(out0))
            pull(in)
        }
      }
      else if (!hasBeenPulled(in)) pull(in)

      override def onDownstreamFinish(): Unit =
        downstreamFinished()
    })

    private def downstreamFinished(): Unit = {
      activeDownstreamCount -= 1
      if (eagerCancel) {
        completeStage()
      } else if (activeDownstreamCount == 0) {
        cancel(in)
      }
    }
  }
}
