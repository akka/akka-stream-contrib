/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.contrib

import akka.stream.scaladsl.{Keep, Source}
import akka.stream.testkit.scaladsl.{TestSink, TestSource}

import scala.collection.immutable
import scala.collection.immutable.Seq
import scala.concurrent.duration._

class AccumulateWhileUnchangedSpec extends BaseStreamSpec {

  "AccumulateWhileUnchanged" should {

    "emit accumulated elements when the given property changes" in {
      val sink = Source(SampleElements.All)
        .via(AccumulateWhileUnchanged(_.value))
        .toMat(TestSink.probe)(Keep.right)
        .run()

      sink.request(42)
      sink.expectNext(SampleElements.Ones, SampleElements.Twos, SampleElements.Threes)
      sink.expectComplete()
    }

    "not emit any value for an empty source" in {
      Source
        .empty[Element]
        .via(AccumulateWhileUnchanged(_.value))
        .runWith(TestSink.probe)
        .request(42)
        .expectComplete()
    }

    "fail on upstream failure" in {
      val (source, sink) = TestSource
        .probe[Element]
        .via(AccumulateWhileUnchanged(_.value))
        .toMat(TestSink.probe)(Keep.both)
        .run()
      sink.request(42)
      source.sendError(new Exception)
      sink.expectError()
    }

    "used with maxElements" should {
      "emit after maxElements or when the property changes" in {
        val sink = Source(SampleElements.All)
          .via(AccumulateWhileUnchanged(_.value, Some(2)))
          .toMat(TestSink.probe)(Keep.right)
          .run()

        sink.request(42)
        sink.expectNext(Seq(SampleElements.E11, SampleElements.E21))
        sink.expectNext(Seq(SampleElements.E31))
        sink.expectNext(Seq(SampleElements.E42, SampleElements.E52))
        sink.expectNext(Seq(SampleElements.E63))
        sink.expectComplete()
      }
    }

    "used with maxDuration" should {
      "emit after maxDuration or when the property changes" in {
        val (src, sink) = TestSource
          .probe[Element]
          .via(AccumulateWhileUnchanged(_.value, maxDuration = Some(500.millis)))
          .toMat(TestSink.probe[Seq[Element]])(Keep.both)
          .run()

        sink.request(42)
        SampleElements.Ones.foreach(src.sendNext)
        sink.expectNoMsg(300.millis)
        sink.expectNext(SampleElements.Ones)
        src.sendComplete()
        sink.expectComplete()
      }

      "emit after maxDuration with backpressure" in {
        val (src, sink) = TestSource
          .probe[Element]
          .via(AccumulateWhileUnchanged(_.value, maxDuration = Some(100.millis)))
          .toMat(TestSink.probe[Seq[Element]])(Keep.both)
          .run()

        // Pull/Push Ones without backpressure
        sink.request(1)
        SampleElements.Ones.foreach(src.sendNext)
        sink.expectNext(SampleElements.Ones)

        // Make more input data available without downstream demand for it
        SampleElements.Twos.foreach(src.sendNext)
        // Wait for longer than maxDuration so the timer expires and Twos are pushed
        sink.expectNoMsg(200.millis)
        SampleElements.Threes.foreach(src.sendNext)
        // Wait for longer than maxDuration so the timer expires.
        // Threes can't be pushed yet since there is no demand.
        sink.expectNoMsg(200.millis)

        // Verify all expected messages arrive at sink
        sink.request(2)
        sink.expectNext(SampleElements.Twos)
        sink.expectNext(SampleElements.Threes)

        src.sendComplete()
        sink.expectComplete()
      }
    }

    "used with maxElements and maxDuration" should {
      "emit without dropping" in {
        val (src, sink) = TestSource
          .probe[Element]
          .via(AccumulateWhileUnchanged(_.value, maxElements = Some(2), maxDuration = Some(500.millis)))
          .toMat(TestSink.probe[Seq[Element]])(Keep.both)
          .run()

        SampleElements.Twos.foreach(src.sendNext)
        sink.request(1)
        sink.expectNext(SampleElements.Twos)

        SampleElements.Ones.foreach(src.sendNext)
        sink.request(1)
        sink.expectNext(SampleElements.Ones.take(2))

        // Complete should return last element of Ones immediately
        src.sendComplete()
        sink.request(1)
        sink.expectNext(Seq(SampleElements.Ones(2)))
        sink.expectComplete()
      }
    }
  }
}

case class Element(id: Int, value: Int)

object SampleElements {

  val E11 = Element(1, 1)
  val E21 = Element(2, 1)
  val E31 = Element(3, 1)
  val E42 = Element(4, 2)
  val E52 = Element(5, 2)
  val E63 = Element(6, 3)

  val Ones = immutable.Seq(E11, E21, E31)
  val Twos = immutable.Seq(E42, E52)
  val Threes = immutable.Seq(E63)

  val All = Ones ++ Twos ++ Threes
}
