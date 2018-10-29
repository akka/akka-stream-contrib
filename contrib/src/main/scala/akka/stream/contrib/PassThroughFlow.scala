/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.contrib

import akka.NotUsed
import akka.stream._
import akka.stream.scaladsl._

/**
 * Given `Flow[In, Out]` and `f: (In, Out) => Result`,
 * transforms original flow into a `Flow[In, Result]`, i.e.
 * for every element `e` signalled from upstream, emits `f(e, flow(e))`.
 *
 * Has an overloaded factory that fixes `f` to be a tuple constructor, i.e.
 * for every element `e` signalled from upstream, emits a tuple `(e, flow(e))`.
 *
 * IMPORTANT!
 * This flow combinator is guaranteed to work correctly on flows
 * that have behavior of classic total functions, meaning that
 * they should not reorder, drop, inject etc new elements.
 * In the future these restrictions may be lifted,
 * for now please refer to the following resources for more:
 * - [[https://github.com/akka/akka-stream-contrib/pull/142#discussion_r228875614]]
 * - [[https://github.com/akka/akka/issues/15957]] and linked issues
 * - [[https://discuss.lightbend.com/t/passing-stream-elements-into-and-over-flow/2536]] and linked resources
 *
 * Applies no internal buffering / flow control etc.
 *
 * '''Emits when''' upstream emits an element
 *
 * '''Backpressures when''' downstream backpressures
 *
 * '''Completes when''' upstream completes
 *
 * '''Cancels when''' downstream cancels
 *
 * Examples:
 *
 * 1. Consuming from Kafka: Allows for busines logic to stay unaware of commits:
 *
 * {{{
 * val logic: Flow[CommittableMessage[String, Array[Byte]], ProcessingResult] =
 *   Flow[CommittableMessage[String, Array[Byte]]]
 *     .map(m => process(m.record))
 *
 * // Used like this:
 * Consumer
 *   .committableSource(settings, Subscriptions.topics("my-topic"))
 *   .via(PassThroughFlow(logic))
 *   .map { case (committableMessage, processingResult) =>
 *     // decide to commit or not based on `processingResult`
 *   }
 *
 * // or:
 *
 * Consumer
 *   .committableSource(settings, Subscriptions.topics("my-topic"))
 *   .via(PassThroughFlow(logic, Keep.left)) // process messages but return original elements
 *   .mapAsync(1)(_.commitScalaDsl())
 * }}}
 *
 * 2. Logging HTTP request-response based on some rule
 * {{{
 * // assuming Akka HTTP entities:
 * val route: Route = ???
 *
 * // Route has an implicit conversion to Flow[HttpRequest, HttpResponse]
 *
 * Flow[HttpRequest]
 *   .map { r =>
 *     // don't log web crawlers' requests
 *     if (userAgent(r) != "google-bot") {
 *       logRequest(r)
 *     }
 *     r
 *   }
 *   .via(PassThroughFlow(route)) // req => (req, resp)
 *   .map { case (req, resp) =>
 *     // don't log responses to web crawlers
 *     if (userAgent(req) != "google-bot") {
 *       logResponse(resp)
 *     }
 *     resp
 *   }
 *  }}}
 */
object PassThroughFlow {
  def apply[I, O](processingFlow: Flow[I, O, NotUsed]): Graph[FlowShape[I, (I, O)], NotUsed] = {
    apply[I, O, (I, O)](processingFlow, Keep.both)
  }

  def apply[I, O, R](
    processingFlow: Flow[I, O, NotUsed],
    output:         (I, O) => R): Graph[FlowShape[I, R], NotUsed] = {
    Flow.fromGraph(GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      val broadcast: UniformFanOutShape[I, I] =
        builder.add(Broadcast[I](2))

      val zip: FanInShape2[I, O, R] =
        builder.add(ZipWith[I, O, R]((left, right) => output(left, right)))

      // format: off
      broadcast.out(0) ~> zip.in0
      broadcast.out(1) ~> processingFlow ~> zip.in1
      // format: on

      FlowShape(broadcast.in, zip.out)
    })
  }
}
