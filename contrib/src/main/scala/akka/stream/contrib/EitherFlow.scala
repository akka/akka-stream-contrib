/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.contrib

import akka.NotUsed
import akka.stream.FlowShape
import akka.stream.scaladsl.{ Broadcast, Flow, GraphDSL, Merge }

object EitherFlow {
  /**
   * Creates a Flow[Either[R, L], Result, NotUsed] that can choose between two flows based on the materialized value
   *
   * @param leftFlow: flow used for left values
   * @param rightFlow: flow used for right values
   * @return merge flow from either leftFlow or rightFlow
   */
  def either[L, R, Result](
    leftFlow:  Flow[L, Result, NotUsed],
    rightFlow: Flow[R, Result, NotUsed]): Flow[Either[L, R], Result, NotUsed] =
    Flow.fromGraph(GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

      val eitherIn = b.add(Broadcast[Either[L, R]](2))
      val result = b.add(Merge[Result](2))

      eitherIn.collect { case Left(v) => v } ~> leftFlow ~> result
      eitherIn.collect { case Right(v) => v } ~> rightFlow ~> result

      FlowShape(eitherIn.in, result.out)
    })
}
