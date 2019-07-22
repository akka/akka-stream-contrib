/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.contrib

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.stream.testkit.{TestPublisher, TestSubscriber}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{MustMatchers, WordSpec}

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._

class TokenThrottleSpec extends WordSpec with MustMatchers with ScalaFutures {

  implicit val system: ActorSystem = ActorSystem()
  implicit val mat: ActorMaterializer = ActorMaterializer()
  implicit val ec: ExecutionContextExecutor = system.dispatcher

  "token throttle" should {

    "let elements pass only when tokens are available" in {
      val (elems, tokens, out) = throttledGraph

      tokens.sendNext(2)
      elems.sendNext(1)
      out.requestNext() mustBe 1
      elems.sendNext(2)
      out.requestNext() mustBe 2
      elems.sendNext(3)
      an[AssertionError] shouldBe thrownBy(out.requestNext(100.millis)) // expect element to be blocked
      tokens.sendNext(1)
      out.requestNext() mustBe 3
    }

    "ask for tokens only when tokens are needed" in {
      val tokenAsked = new AtomicInteger()
      val tokens = Source.repeat(10L).take(20).alsoTo(Sink.foreach(_ => tokenAsked.incrementAndGet()))

      Source
        .repeat(1)
        .take(25)
        .via(TokenThrottle(tokens)(_ => 1))
        .runWith(Sink.ignore)
        .futureValue

      tokenAsked.get() mustBe 3
    }

    "consume tokens according to cost" in {
      val tokenAsked = new AtomicInteger()
      val tokens = Source.repeat(1L).alsoTo(Sink.foreach(_ => tokenAsked.incrementAndGet()))

      val sum = Source
        .fromIterator(() => Stream.from(1, 1).iterator)
        .take(40)
        .via(TokenThrottle(tokens)(_.toLong))
        .runWith(Sink.fold(0)(_ + _))
        .futureValue

      tokenAsked.get() mustBe sum
    }

    "complete when all tokens are consumed" in {
      val (elems, tokens, out) = throttledGraph

      tokens.sendNext(2)
      elems.sendNext(1)
      out.requestNext() mustBe 1
      tokens.sendComplete()

      elems.sendNext(2)
      out.requestNext() mustBe 2
      out.expectComplete()
    }

    "complete when elements are consumed" in {

      val (elems, tokens, out) = throttledGraph

      tokens.sendNext(10)
      elems.sendNext(1)
      out.requestNext() mustBe 1
      elems.sendNext(2)
      out.requestNext() mustBe 2
      elems.sendComplete()
      out.expectComplete()
    }

    "completes if element is buffered and token source completes with too few remaining tokens" in {
      val ((elems, tokens), out) = TestSource
        .probe[Int]
        .viaMat(TokenThrottle(TestSource.probe[Long])(_ => 5))(Keep.both)
        .toMat(TestSink.probe)(Keep.both)
        .run()

      tokens.sendNext(8)
      elems.sendNext(1)
      elems.sendNext(2)
      out.requestNext() mustBe 1
      tokens.sendComplete()
      out.expectComplete()
    }

    "asks for tokens to satisfy current item cost even if downstream did not yet request" in {
      val ((elems, tokens), out) = TestSource
        .probe[Int]
        .viaMat(TokenThrottle(TestSource.probe[Long])(_ => 100))(Keep.both)
        .toMat(TestSink.probe)(Keep.both)
        .run()

      elems.sendNext(1)
      for (_ <- 1 to 100) {
        if (tokens.pending == 0) tokens.expectRequest()
        tokens.pending mustBe >=(1L)
        tokens.sendNext(1)
      }
      out.requestNext() mustBe 1
    }
  }

  def throttledGraph: (TestPublisher.Probe[Int], TestPublisher.Probe[Long], TestSubscriber.Probe[Int]) = {
    val ((elems, tokens), out) = TestSource
      .probe[Int]
      .viaMat(TokenThrottle(TestSource.probe[Long])(_ => 1))(Keep.both)
      .toMat(TestSink.probe)(Keep.both)
      .run()
    (elems, tokens, out)
  }
}
