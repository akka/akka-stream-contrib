/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.contrib

import akka.stream.contrib.DelayFlow.DelayStrategy
import akka.stream.scaladsl.Source
import scala.language.postfixOps
import akka.stream.testkit.scaladsl.TestSink
import scala.concurrent.duration._
import akka.testkit._

class DelayFlowSpecAutoFusingOn extends { val autoFusing = true } with DelayFlowSpec
class DelayFlowSpecAutoFusingOff extends { val autoFusing = false } with DelayFlowSpec

trait DelayFlowSpec extends BaseStreamSpec {

  "DelayFlow" should {

    "work with empty source" in {
      Source.empty[Int]
        .via(DelayFlow(Duration.Zero))
        .runWith(TestSink.probe)
        .request(1)
        .expectComplete()
    }

    "work with fixed delay" in {

      val fixedDelay = 1 second

      val elems = 1 to 10

      val probe = Source(elems)
        .map(_ => System.nanoTime())
        .via(DelayFlow[Long](fixedDelay))
        .map(start => System.nanoTime() - start)
        .runWith(TestSink.probe)

      elems.foreach(_ => {
        val next = probe
          .request(1)
          .expectNext(fixedDelay + fixedDelay.dilated)
        next should be >= fixedDelay.toNanos
      })

      probe.expectComplete()

    }

    "work without delay" in {

      val elems = Vector(1, 2, 3, 4, 5, 6, 7, 8, 9, 0)

      Source(elems)
        .via(DelayFlow[Int](Duration.Zero))
        .runWith(TestSink.probe)
        .request(elems.size)
        .expectNextN(elems)
        .expectComplete()
    }

    "work with linear increasing delay" in {

      val elems = 1 to 10
      val step = 1 second
      val initial = 1 second
      val max = 5 seconds

      def incWhile(i: (Int, Long)): Boolean = i._1 < 7

      val probe = Source(elems)
        .map(e => (e, System.nanoTime()))
        .via(DelayFlow[(Int, Long)](() => DelayStrategy
          .linearIncreasingDelay(step, incWhile, initial, max)))
        .map(start => System.nanoTime() - start._2)
        .runWith(TestSink.probe)

      elems.foreach(e =>
        if (incWhile((e, 1L))) {
          val afterIncrease = initial + e * step
          val delay = if (afterIncrease < max) {
            afterIncrease
          } else {
            max
          }
          val next = probe
            .request(1)
            .expectNext(delay + delay.dilated)
          next should be >= delay.toNanos
        } else {
          val next = probe
            .request(1)
            .expectNext(initial + initial.dilated)
          next should be >= initial.toNanos
        })

      probe.expectComplete()

    }

  }

}
