/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.contrib

import java.util.concurrent.ThreadFactory

import akka.event.LoggingAdapter
import akka.stream.scaladsl.Source
import akka.stream.testkit.scaladsl.TestSink
import com.miguno.akka.testing.{ MockScheduler, VirtualTime }
import com.typesafe.config.{ Config, ConfigFactory }

import scala.concurrent.duration._

class TimeWindowSpecAutoFusingOff extends { val autoFusing = false } with TimeWindowSpec
class TimeWindowSpecAutoFusingOn extends { val autoFusing = true } with TimeWindowSpec

class AkkaMockScheduler extends {
  val time = new VirtualTime
} with MockScheduler(time) {
  def this(config: Config, adapter: LoggingAdapter, tf: ThreadFactory) = this()
}

trait TimeWindowSpec extends BaseStreamSpec {

  override def config = ConfigFactory.parseString(
    s"""
      |akka.scheduler.implementation = ${classOf[AkkaMockScheduler].getName}
    """.stripMargin
  )

  private val timeWindow = 100.millis
  private val epsilonTime = 10.millis

  private val scheduler = system.scheduler.asInstanceOf[AkkaMockScheduler]

  "TimeWindow flow" should {
    "aggregate data for predefined amount of time" in {
      val summingWindow = TimeWindow(timeWindow, eager = false)(identity[Int])(_ + _)

      val sub = Source.repeat(1)
        .via(summingWindow)
        .runWith(TestSink.probe)

      sub.request(2)

      sub.expectNoMsg(timeWindow + epsilonTime)
      scheduler.time.advance(timeWindow + epsilonTime)
      scheduler.tick()
      sub.expectNext()

      sub.expectNoMsg(timeWindow + epsilonTime)
      scheduler.time.advance(timeWindow + epsilonTime)
      scheduler.tick()
      sub.expectNext()
    }

    "emit the first seed if eager" in {
      val summingWindow = TimeWindow(timeWindow, eager = true)(identity[Int])(_ + _)

      val sub = Source.repeat(1)
        .via(summingWindow)
        .runWith(TestSink.probe)

      sub.request(2)

      sub.expectNext()

      sub.expectNoMsg(timeWindow + epsilonTime)
      scheduler.time.advance(timeWindow + epsilonTime)
      scheduler.tick()
      sub.expectNext()
    }
  }
}
