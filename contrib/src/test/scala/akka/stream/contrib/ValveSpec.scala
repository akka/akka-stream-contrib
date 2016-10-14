/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.contrib

import akka.actor.ActorSystem
import akka.pattern.after
import akka.stream.ActorMaterializer
import akka.stream.contrib.SwitchMode.{Close, Open}
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.testkit.scaladsl._
import org.scalatest._
import org.scalatest.Matchers._
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.Future
import scala.concurrent.duration._

class ValveSpec extends WordSpec with ScalaFutures {

  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()
  implicit val executionContext = materializer.executionContext

  "A closed valve" should {

    "emit only 3 elements into a sequence when the valve is switched to open" in {

      val (switch, seq) = Source(1 to 3)
        .viaMat(new Valve(SwitchMode.Close))(Keep.right)
        .toMat(Sink.seq)(Keep.both)
        .run()

      after(100 millis, system.scheduler) {
        val future: Future[Boolean] = switch.flip(Open)
        whenReady(future, timeout(200 millis)) {
          _ shouldBe true
        }

        future
      }

      whenReady(seq, timeout(200 millis)) {
        _ should contain inOrder(1, 2, 3)
      }
    }

    "emit only 5 elements when the valve is switched to open" in {
      val (switch, probe) = Source(1 to 5)
        .viaMat(new Valve(SwitchMode.Close))(Keep.right)
        .toMat(TestSink.probe[Int])(Keep.both)
        .run()

      probe.request(2)
      probe.expectNoMsg(100 millis)

      whenReady(switch.flip(Open)) {
        _ shouldBe true
      }

      probe.expectNext shouldBe 1
      probe.expectNext shouldBe 2

      probe.request(3)
      probe.expectNext shouldBe 3
      probe.expectNext shouldBe 4
      probe.expectNext shouldBe 5

      probe.expectComplete()
    }


    "emit only 3 elements when the valve is switch to open/close/open" in {
      val ((sourceProbe, switch), sinkProbe) = TestSource.probe[Int]
        .viaMat(Valve())(Keep.both)
        .toMat(TestSink.probe[Int])(Keep.both)
        .run()


      sinkProbe.request(1)
      whenReady(switch.flip(Close)) {
        _ shouldBe true
      }
      sourceProbe.sendNext(1)
      sinkProbe.expectNoMsg(100 millis)

      whenReady(switch.flip(Open)) {
        _ shouldBe true
      }
      sinkProbe.expectNext shouldEqual 1

      whenReady(switch.flip(Close)) {
        _ shouldBe true
      }
      whenReady(switch.flip(Open)) {
        _ shouldBe true
      }
      sinkProbe.expectNoMsg(100 millis)

      sinkProbe.request(1)
      sinkProbe.request(1)
      sourceProbe.sendNext(2)
      sourceProbe.sendNext(3)
      sourceProbe.sendComplete()

      sinkProbe.expectNext shouldBe 2
      sinkProbe.expectNext shouldBe 3

      sinkProbe.expectComplete()
    }

    "return false when the valve is already closed" in {
      val (switch, probe) = Source(1 to 5)
        .viaMat(Valve(SwitchMode.Close))(Keep.right)
        .toMat(TestSink.probe[Int])(Keep.both)
        .run()

      whenReady(switch.flip(Close)) { element =>
        element should be(false)
      }
      whenReady(switch.flip(Close)) { element =>
        element should be(false)
      }
    }

    "emit nothing when the source is empty" in {
      val (switch, seq) = Source.empty
        .viaMat(Valve(SwitchMode.Close))(Keep.right)
        .toMat(Sink.seq)(Keep.both)
        .run()


      whenReady(seq, timeout(200 millis)) {
        _ shouldBe empty
      }
    }

    "emit nothing when the source is failing" in {
      val (switch, seq) = Source.failed(new IllegalArgumentException("Fake exception"))
        .viaMat(Valve(SwitchMode.Close))(Keep.right)
        .toMat(Sink.seq)(Keep.both)
        .run()

      whenReady(seq.failed) { e =>
        e shouldBe an[IllegalArgumentException]
      }
    }

  }

  "A opened valve" should {

    "emit 5 elements after it has been close/open" in {
      val (switch, probe) = Source(1 to 5)
        .viaMat(Valve())(Keep.right)
        .toMat(TestSink.probe[Int])(Keep.both)
        .run()

      probe.request(2)
      probe.expectNext() shouldBe 1
      probe.expectNext() shouldBe 2

      whenReady(switch.flip(Close)) {
        _ shouldBe true
      }

      probe.request(1)
      probe.expectNoMsg(100 millis)

      whenReady(switch.flip(Open)) {
        _ shouldBe true
      }
      probe.expectNext() shouldBe 3

      probe.request(2)
      probe.expectNext() shouldBe 4
      probe.expectNext() shouldBe 5

      probe.expectComplete()
    }

    "return false when the valve is already opened" in {
      val (switch, probe) = Source(1 to 5)
        .viaMat(Valve())(Keep.right)
        .toMat(TestSink.probe[Int])(Keep.both)
        .run()

      whenReady(switch.flip(Open)) {
        _ shouldBe false
      }
      whenReady(switch.flip(Open)) {
        _ shouldBe false
      }
    }

    "emit only 3 elements into a sequence" in {

      val (switch, seq) = Source(1 to 3)
        .viaMat(Valve())(Keep.right)
        .toMat(Sink.seq)(Keep.both)
        .run()

      whenReady(seq, timeout(200 millis)) {
        _ should contain inOrder(1, 2, 3)
      }
    }

    "emit nothing when the source is empty" in {
      val (switch, seq) = Source.empty
        .viaMat(Valve())(Keep.right)
        .toMat(Sink.seq)(Keep.both)
        .run()


      whenReady(seq, timeout(200 millis)) {
        _ shouldBe empty
      }

    }

    "emit nothing when the source is failing" in {
      val (switch, seq) = Source.failed(new IllegalArgumentException("Fake exception"))
        .viaMat(Valve())(Keep.right)
        .toMat(Sink.seq)(Keep.both)
        .run()

      whenReady(seq.failed) { e =>
        e shouldBe an[IllegalArgumentException]
      }
    }
  }
}
