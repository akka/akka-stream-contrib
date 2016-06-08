/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.contrib

import akka.stream.scaladsl.{Keep, Source}
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.stream.{Attributes, DelayOverflowStrategy}

import scala.concurrent.duration._

class SortWithinSpecAutoFusingOn extends {
  val autoFusing = true
} with SortWithinSpec

class SortWithinSpecAutoFusingOff extends {
  val autoFusing = false
} with SortWithinSpec

trait SortWithinSpec extends BaseStreamSpec {

  "A stream via SortWithin" should {
    "sort the elements published till given FiniteDuration" in {
      val (source, sink) = TestSource.probe[Integer]
        .via(SortWithin[Integer](500.millis))
        .toMat(TestSink.probe)(Keep.both)
        .run()
      sink.request(99)
      source.sendNext(1)
      source.sendNext(3)
      source.sendNext(2)
      sink.expectNext(1200.millis, 1)
      sink.expectNext(1200.millis, 2)
      sink.expectNext(1200.millis, 3)
    }

    "sort the elements till upstream completion" in {
      val (source, sink) = TestSource.probe[Integer]
        .via(SortWithin[Integer](600.millis))
        .toMat(TestSink.probe)(Keep.both)
        .run()
      sink.request(99)
      source.sendNext(1)
      source.sendNext(3)
      source.sendNext(1)
      source.sendComplete()
      sink.expectNext(1, 1, 3)
      sink.expectComplete()
    }

    "sort elements for every finite duration" in {

      Source((1 to 10).reverse).map(Integer.valueOf).delay(200.millis, DelayOverflowStrategy.backpressure)
        .withAttributes(Attributes.inputBuffer(1, 1))
        .via(SortWithin[Integer](900.millis))
        .runWith(TestSink.probe[Integer])
        .request(10)
        .expectNoMsg(800.millis)
        .expectNext(7, 8, 9, 10)
        .expectNoMsg(800.millis)
        .expectNext(3, 4, 5, 6)
        .expectNext(1, 2)
        .expectComplete()
    }
  }
}