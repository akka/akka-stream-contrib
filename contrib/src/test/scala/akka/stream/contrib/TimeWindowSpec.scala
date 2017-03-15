/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.contrib

import akka.stream.scaladsl.Keep
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.testkit.TestDuration
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.duration._

/*
conflateWithSeed seems not to work as expected because of buffering of each stage when auto-fusing=off
class TimeWindowSpecAutoFusingOff extends { val autoFusing = false } with TimeWindowSpec
 */
class TimeWindowSpecAutoFusingOn extends { val autoFusing = true } with TimeWindowSpec

trait TimeWindowSpec extends BaseStreamSpec with ScalaFutures {
  private val timeWindow = 500.milliseconds

  "TimeWindow flow" should {
    "aggregate data for predefined amount of time" in {
      val summingWindow = TimeWindow(timeWindow.dilated, eager = false)(identity[Int])(_ + _)

      val (pub, sub) = TestSource.probe[Int]
        .via(summingWindow)
        .toMat(TestSink.probe)(Keep.both)
        .run

      sub.request(2)

      pub.sendNext(1)
      pub.sendNext(1)
      pub.sendNext(1)
      pub.sendNext(1)
      pub.sendNext(1)
      sub.expectNext(timeWindow, 5)
      pub.sendNext(1)
      pub.sendNext(1)
      pub.sendNext(1)
      pub.sendNext(1)
      pub.sendNext(1)
      sub.expectNext(timeWindow, 5)
    }

    "emit the first seed if eager" in {
      val summingWindow = TimeWindow(timeWindow.dilated, eager = true)(identity[Int])(_ + _)

      val (pub, sub) = TestSource.probe[Int]
        .via(summingWindow)
        .toMat(TestSink.probe)(Keep.both)
        .run

      sub.request(2)

      pub.sendNext(1)
      sub.expectNext(1)
      pub.sendNext(1)
      pub.sendNext(1)
      pub.sendNext(1)
      pub.sendNext(1)
      pub.sendNext(1)
      sub.expectNext(5)
    }
  }
}
