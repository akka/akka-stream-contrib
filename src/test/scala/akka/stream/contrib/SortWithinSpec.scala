/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.contrib

import java.util.concurrent.TimeUnit

import akka.stream.scaladsl.Keep
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
  }

  "A stream via SortWithin" should {
    "sort the elements till upstream completion" in {
      val (source, sink) = TestSource.probe[Integer]
        .via(SortWithin[Integer](FiniteDuration.apply(20, TimeUnit.SECONDS)))
        .toMat(TestSink.probe)(Keep.both)
        .run()
      sink.request(99)
      source.sendNext(1)
      source.sendNext(3)
      source.sendNext(2)
      source.sendComplete()
      sink.expectNext(1, 2, 3)
      sink.expectComplete()
    }
  }

  "A stream via SortWithin" should {
    "sort the elements till exception" in {
      val (source, sink) = TestSource.probe[Integer]
        .via(SortWithin[Integer](FiniteDuration.apply(2, TimeUnit.SECONDS)))
        .toMat(TestSink.probe)(Keep.both)
        .run()
      sink.request(99)
      source.sendNext(3)
      source.sendNext(1)
      val cause: RuntimeException = new RuntimeException
      source.sendError(cause)
      sink.expectNext(FiniteDuration.apply(3, TimeUnit.SECONDS), 1)
      sink.expectError(cause)
    }
  }
}
