/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.contrib

import java.util.concurrent.TimeUnit

import akka.stream.DelayOverflowStrategy
import akka.stream.scaladsl.{Keep, Source}
import akka.stream.testkit.scaladsl.{TestSink, TestSource}

import scala.concurrent.duration.FiniteDuration

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
        .via(SortWithin[Integer](FiniteDuration.apply(5, TimeUnit.SECONDS)))
        .toMat(TestSink.probe)(Keep.both)
        .run()
      sink.request(99)
      source.sendNext(1)
      source.sendNext(3)
      source.sendNext(2)
      sink.expectNext(FiniteDuration.apply(6, TimeUnit.SECONDS), 1)
      sink.expectNext(FiniteDuration.apply(6, TimeUnit.SECONDS), 2)
      sink.expectNext(FiniteDuration.apply(6, TimeUnit.SECONDS), 3)
    }

    "sort the elements till upstream completion" in {
      val (source, sink) = TestSource.probe[Integer]
        .via(SortWithin[Integer](FiniteDuration.apply(20, TimeUnit.SECONDS)))
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
      val duration: FiniteDuration = FiniteDuration.apply(1, TimeUnit.SECONDS)
      Source((1 to 10).reverse).map(Integer.valueOf).delay(duration, DelayOverflowStrategy.backpressure)
        .via(SortWithin[Integer](FiniteDuration.apply(5, TimeUnit.SECONDS)))
        .runWith(TestSink.probe[Integer])
        .request(10)
        .expectNoMsg(FiniteDuration.apply(4700, TimeUnit.MILLISECONDS))
        .expectNext(7, 8, 9, 10)
        .expectNoMsg(FiniteDuration.apply(4600, TimeUnit.MILLISECONDS))
        .expectNext(2, 3, 4, 5, 6)
        .expectNext(1)
        .expectComplete()
    }
  }
}