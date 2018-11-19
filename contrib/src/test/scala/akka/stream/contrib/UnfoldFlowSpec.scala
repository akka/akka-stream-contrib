/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.contrib

import akka.stream.scaladsl._
import akka.stream.OverflowStrategies
import akka.stream.testkit.{ TestPublisher, TestSubscriber }
import akka.stream.testkit.scaladsl.{ TestSink, TestSource }
import scala.concurrent.duration.DurationInt
import akka.util.Timeout

class UnfoldFlowSpecAutoFusingOn extends { val autoFusing = true } with UnfoldFlowSpec
class UnfoldFlowSpecAutoFusingOff extends { val autoFusing = false } with UnfoldFlowSpec

trait UnfoldFlowSpec extends BaseStreamSpec {
  implicit val unfoldTimeout: Timeout = 300.millis

  "unfoldFlow" should {

    "unfold Collatz conjecture with a sequence of 111 elements" should {

      val done = new Exception("done")
      val outputs = List(27, 82, 41, 124, 62, 31, 94, 47, 142, 71, 214, 107, 322, 161, 484, 242, 121, 364, 182, 91, 274, 137, 412, 206, 103, 310, 155, 466, 233, 700, 350, 175, 526, 263, 790, 395, 1186, 593, 1780, 890, 445, 1336, 668, 334, 167, 502, 251, 754, 377, 1132, 566, 283, 850, 425, 1276, 638, 319, 958, 479, 1438, 719, 2158, 1079, 3238, 1619, 4858, 2429, 7288, 3644, 1822, 911, 2734, 1367, 4102, 2051, 6154, 3077, 9232, 4616, 2308, 1154, 577, 1732, 866, 433, 1300, 650, 325, 976, 488, 244, 122, 61, 184, 92, 46, 23, 70, 35, 106, 53, 160, 80, 40, 20, 10, 5, 16, 8, 4, 2)

      "with flow" in {

        val source = SourceGen.unfoldFlow(27)(Flow.fromFunction[Int, (Int, Int)] {
          case 1               => throw done
          case n if n % 2 == 0 => (n / 2, n)
          case n               => (n * 3 + 1, n)
        }.recover {
          case `done` => (1, 1)
        })

        val sink = source.runWith(TestSink.probe)

        outputs.foreach { output =>
          sink.request(1)
          sink.expectNext(output)
        }
        sink.request(1)
        sink.expectNext(1)
        sink.expectComplete()
      }

      "with buffered flow" in {

        def bufferedSource(buffSize: Int) = SourceGen.unfoldFlow(27)(Flow.fromFunction[Int, (Int, Int)] {
          case 1               => throw done
          case n if n % 2 == 0 => (n / 2, n)
          case n               => (n * 3 + 1, n)
        }.recover {
          case `done` => (1, 1)
        }.buffer(buffSize, OverflowStrategies.Backpressure))

        val sink = bufferedSource(10).runWith(TestSink.probe)

        sink.request(outputs.length.toLong)
        outputs.foreach(sink.expectNext(_))
        sink.request(1)
        sink.expectNext(1)
        sink.expectComplete()
      }

      "with function" in {

        val source = SourceGen.unfoldFlowWith(27, Flow.fromFunction(identity[Int])) {
          case 1               => None
          case n if n % 2 == 0 => Some((n / 2, n))
          case n               => Some((n * 3 + 1, n))
        }

        val sink = source.runWith(TestSink.probe)
        outputs.foreach { output =>
          sink.request(1)
          sink.expectNext(output)
        }
        sink.request(1)
        sink.expectComplete()
      }
    }

    "increment integers & handle KillSwitch" should {
      type SProbe = TestSubscriber.Probe[Int]
      val timeout = 300.millis

      "with simple flow" should {

        type PProbe = TestPublisher.Probe[(Int, Int)]
        val controlledFlow = Flow.fromSinkAndSourceMat[Int, (Int, Int), SProbe, PProbe, (SProbe, PProbe)](TestSink.probe[Int], TestSource.probe[(Int, Int)])(Keep.both)
        val source = SourceGen.unfoldFlow(1)(controlledFlow)

        "fail instantly when aborted" in {
          val ((sub, pub), snk) = source.toMat(TestSink.probe[Int])(Keep.both).run()
          val kill = new Exception("KILL!")
          sub.ensureSubscription()
          pub.ensureSubscription()
          snk.ensureSubscription()
          pub.sendError(kill)
          snk.expectError(kill)
        }

        "fail after timeout when aborted" in {
          val ((sub, pub), snk) = source.toMat(TestSink.probe[Int])(Keep.both).run()
          sub.ensureSubscription()
          pub.ensureSubscription()
          snk.ensureSubscription()
          sub.cancel()
          snk.expectNoMsg(timeout - 50.millis)
          snk.expectError()
        }

        "fail when inner stream is canceled and pulled before completion" in {
          val ((sub, pub), snk) = source.toMat(TestSink.probe[Int])(Keep.both).run()
          sub.ensureSubscription()
          pub.ensureSubscription()
          snk.ensureSubscription()
          sub.cancel()
          snk.request(1)
          snk.expectNoMsg(timeout - 50.millis)
          snk.expectError()
        }

        "fail when inner stream is canceled, pulled before completion, and finally aborted" in {
          val ((sub, pub), snk) = source.toMat(TestSink.probe[Int])(Keep.both).run()
          val kill = new Exception("KILL!")
          sub.ensureSubscription()
          pub.ensureSubscription()
          snk.ensureSubscription()
          sub.cancel()
          snk.request(1)
          pub.sendError(kill)
          snk.expectError(kill)
        }

        "fail after 3 elements when aborted" in {
          val ((sub, pub), snk) = source.toMat(TestSink.probe[Int])(Keep.both).run()
          val kill = new Exception("KILL!")
          sub.ensureSubscription()
          pub.ensureSubscription()
          snk.ensureSubscription()
          snk.request(1)
          sub.requestNext(1)
          pub.sendNext(2 -> 1)
          snk.expectNext(1)
          snk.request(1)
          sub.requestNext(2)
          pub.sendNext(3 -> 2)
          snk.expectNext(2)
          snk.request(1)
          sub.requestNext(3)
          pub.sendNext(4 -> 3)
          snk.expectNext(3)
          pub.sendError(kill)
          snk.expectError(kill)
        }

        "complete gracefully instantly when stopped" in {
          val ((sub, pub), snk) = source.toMat(TestSink.probe[Int])(Keep.both).run()
          sub.ensureSubscription()
          pub.ensureSubscription()
          snk.ensureSubscription()
          pub.sendComplete()
          snk.expectComplete()
        }

        "complete gracefully after timeout when stopped" in {
          val ((sub, pub), snk) = source.toMat(TestSink.probe[Int])(Keep.both).run()
          sub.ensureSubscription()
          pub.ensureSubscription()
          snk.ensureSubscription()
          sub.cancel()
          snk.expectNoMsg(timeout - 50.millis)
          pub.sendComplete()
          snk.expectComplete()
        }

        "complete gracefully after 3 elements when stopped" in {
          val ((sub, pub), snk) = source.toMat(TestSink.probe[Int])(Keep.both).run()
          sub.ensureSubscription()
          pub.ensureSubscription()
          snk.ensureSubscription()
          snk.request(1)
          sub.requestNext(1)
          pub.sendNext(2 -> 1)
          snk.expectNext(1)
          snk.request(1)
          sub.requestNext(2)
          pub.sendNext(3 -> 2)
          snk.expectNext(2)
          snk.request(1)
          sub.requestNext(3)
          pub.sendNext(4 -> 3)
          snk.expectNext(3)
          pub.sendComplete()
          snk.expectComplete()
        }
      }

      "with function" should {

        type PProbe = TestPublisher.Probe[Int]
        val controlledFlow = Flow.fromSinkAndSourceMat[Int, Int, SProbe, PProbe, (SProbe, PProbe)](TestSink.probe[Int], TestSource.probe[Int])(Keep.both)
        val source = SourceGen.unfoldFlowWith(1, controlledFlow) { n =>
          Some((n + 1, n))
        }

        "fail instantly when aborted" in {
          val ((sub, pub), snk) = source.toMat(TestSink.probe[Int])(Keep.both).run()
          val kill = new Exception("KILL!")
          sub.ensureSubscription()
          pub.ensureSubscription()
          snk.ensureSubscription()
          pub.sendError(kill)
          snk.expectError(kill)
        }

        "fail after timeout when aborted" in {
          val ((sub, pub), snk) = source.toMat(TestSink.probe[Int])(Keep.both).run()
          sub.ensureSubscription()
          pub.ensureSubscription()
          snk.ensureSubscription()
          sub.cancel()
          snk.expectNoMsg(timeout - 50.millis)
          snk.expectError()
        }

        "fail when inner stream is canceled and pulled before completion" in {
          val ((sub, pub), snk) = source.toMat(TestSink.probe[Int])(Keep.both).run()
          sub.ensureSubscription()
          pub.ensureSubscription()
          snk.ensureSubscription()
          sub.cancel()
          snk.request(1)
          snk.expectNoMsg(timeout - 50.millis)
          snk.expectError()
        }

        "fail when inner stream is canceled, pulled before completion, and finally aborted" in {
          val ((sub, pub), snk) = source.toMat(TestSink.probe[Int])(Keep.both).run()
          val kill = new Exception("KILL!")
          sub.ensureSubscription()
          pub.ensureSubscription()
          snk.ensureSubscription()
          sub.cancel()
          snk.request(1)
          pub.sendError(kill)
          snk.expectError(kill)
        }

        "fail after 3 elements when aborted" in {
          val ((sub, pub), snk) = source.toMat(TestSink.probe[Int])(Keep.both).run()
          val kill = new Exception("KILL!")
          sub.ensureSubscription()
          pub.ensureSubscription()
          snk.ensureSubscription()
          snk.request(1)
          sub.requestNext(1)
          pub.sendNext(1)
          snk.expectNext(1)
          snk.request(1)
          sub.requestNext(2)
          pub.sendNext(2)
          snk.expectNext(2)
          snk.request(1)
          sub.requestNext(3)
          pub.sendNext(3)
          snk.expectNext(3)
          pub.sendError(kill)
          snk.expectError(kill)
        }

        "complete gracefully instantly when stopped" in {
          val ((sub, pub), snk) = source.toMat(TestSink.probe[Int])(Keep.both).run()
          sub.ensureSubscription()
          pub.ensureSubscription()
          snk.ensureSubscription()
          pub.sendComplete()
          snk.expectComplete()
        }

        "complete gracefully after timeout when stopped" in {
          val ((sub, pub), snk) = source.toMat(TestSink.probe[Int])(Keep.both).run()
          sub.ensureSubscription()
          pub.ensureSubscription()
          snk.ensureSubscription()
          sub.cancel()
          snk.expectNoMsg(timeout - 50.millis)
          pub.sendComplete()
          snk.expectComplete()
        }

        "complete gracefully after 3 elements when stopped" in {
          val ((sub, pub), snk) = source.toMat(TestSink.probe[Int])(Keep.both).run()
          sub.ensureSubscription()
          pub.ensureSubscription()
          snk.ensureSubscription()
          snk.request(1)
          sub.requestNext(1)
          pub.sendNext(1)
          snk.expectNext(1)
          snk.request(1)
          sub.requestNext(2)
          pub.sendNext(2)
          snk.expectNext(2)
          snk.request(1)
          sub.requestNext(3)
          pub.sendNext(3)
          snk.expectNext(3)
          pub.sendComplete()
          snk.expectComplete()
        }
      }
    }
  }
}
