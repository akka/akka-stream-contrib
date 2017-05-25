/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.contrib

import akka.NotUsed
import akka.stream.ThrottleMode
import akka.stream.scaladsl.Source
import akka.stream.testkit.TestSubscriber.{ OnComplete, OnNext }
import akka.stream.testkit.scaladsl.TestSink
import org.scalatest.Matchers

import scala.concurrent.duration.{ FiniteDuration, _ }

class IntervalBasedRateLimiterAutoFusingOn extends { val autoFusing = true } with IntervalBasedRateLimiterSpec

class IntervalBasedRateLimiterAutoFusingOff extends { val autoFusing = false } with IntervalBasedRateLimiterSpec

trait IntervalBasedRateLimiterSpec extends IntervalBasedThrottlerTestKit {

  "IntervalBasedRateLimiter" should {
    "limit rate of messages" when {
      "frequency is low (1 element per 500ms)" in testCase(
        source = infiniteSource,
        numOfElements = 6,
        maxBatchSize = 1,
        minInterval = 500.millis
      )

      "frequency is medium (10 elements per 100ms)" in testCase(
        source = infiniteSource,
        numOfElements = 300,
        maxBatchSize = 10,
        minInterval = 100.millis
      )

      "frequency is moderate (20 elements per 100ms)" in testCase(
        source = infiniteSource,
        numOfElements = 600,
        maxBatchSize = 20,
        minInterval = 100.millis
      )

      "frequency is moderate (200 elements per 1000ms)" in testCase(
        source = infiniteSource,
        numOfElements = 600,
        maxBatchSize = 200,
        minInterval = 1000.millis
      )

      "frequency is high (200 elements per 100ms)" in testCase(
        source = infiniteSource,
        numOfElements = 6000,
        maxBatchSize = 200,
        minInterval = 100.millis
      )

      "frequency is high (2 000 elements per 1 000ms)" in testCase(
        source = infiniteSource,
        numOfElements = 6000,
        maxBatchSize = 2000,
        minInterval = 1000.millis
      )

      "frequency is very high (50 000 elements per 1 000ms)" in testCase(
        source = infiniteSource,
        numOfElements = 150000,
        maxBatchSize = 50000,
        minInterval = 1000.millis
      )

      "source is slow" in testCase(
        source = slowInfiniteSource(300.millis),
        numOfElements = 10,
        maxBatchSize = 1,
        minInterval = 100.millis
      )
    }
  }

}

trait IntervalBasedThrottlerTestKit extends BaseStreamSpec {
  this: Matchers =>

  type Batch = Seq[Int]

  def testCase(
    source:        => Source[Int, _],
    numOfElements: Int,
    maxBatchSize:  Int,
    minInterval:   FiniteDuration
  ): Unit = {

    val flow = source
      .take(numOfElements.toLong)
      .via(new IntervalBasedRateLimiter[Int](minInterval, maxBatchSize))
      .map { batch =>
        (System.currentTimeMillis(), batch)
      }.runWith(TestSink.probe[(Long, Seq[Int])])

    val (timestamps, batches) = {

      def collectTimestampsAndBatches(acc: List[(Long, Batch)]): List[(Long, Batch)] = {
        flow.request(1)
        flow.expectEventPF {
          case OnNext(batch: (Long, Batch)) =>
            collectTimestampsAndBatches(batch :: acc)
          case OnComplete | _ =>
            acc.reverse
        }
      }

      collectTimestampsAndBatches(Nil)
    }.unzip

    val intervals: Seq[FiniteDuration] = timestamps.sliding(2, 1).map {
      case List(first, second) => (second - first).millis
    }.toList

    intervals.foreach {
      _ should be >= minInterval
    }

    batches.flatten should contain theSameElementsInOrderAs (1 to numOfElements).inclusive
    batches.size shouldBe (numOfElements / maxBatchSize)
  }

  protected def infiniteSource: Source[Int, NotUsed] = Source(Stream.from(1, 1))

  protected def slowInfiniteSource(pushDelay: FiniteDuration): Source[Int, NotUsed] =
    infiniteSource.throttle(1, pushDelay, 1, ThrottleMode.shaping)

}
