/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.contrib

import akka.stream.{ Attributes, OverflowStrategy }
import akka.stream.scaladsl.{ Flow, Keep, Source }
import akka.stream.testkit.scaladsl.{ TestSink, TestSource }
import scala.concurrent.duration._

class FeedbackLoopSpecAutoFusingOn extends { val autoFusing = true } with FeedbackLoopSpec

class FeedbackLoopSpecAutoFusingOff extends { val autoFusing = false } with FeedbackLoopSpec

trait FeedbackLoopSpec extends BaseStreamSpec {

  "Feedback" should {
    "not deadlock on slow downstream" in {
      val N = 1000

      val forwardFlow = PartitionWith((i: Int) => if (i % 2 == 1) Left(i / 2) else Right(i))
      val feedbackArc = Flow[Int]
      val testedFlow = FeedbackLoop(forwardFlow, feedbackArc, N / 2)(Keep.none)

      val sub = Source(1 to N)
        .via(testedFlow)
        .via(Flow[Int].delay(1.millis, OverflowStrategy.backpressure))
        .toMat(TestSink.probe[Int])(Keep.right)
        .run()

      sub.request(n = N.toLong)
      sub.expectNextN(N.toLong)
    }

    "not deadlock on slow feedback arc" in {
      val bufferSize = 8

      val forwardFlow = PartitionWith((i: Long) => if (i % 2 == 1) Left(i / 2) else Right(i))
      val feedbackArc = Flow[Long]
        .delay(1.millis, OverflowStrategy.backpressure)
        .withAttributes(Attributes.inputBuffer(bufferSize, bufferSize))
      val testedFlow = FeedbackLoop(forwardFlow, feedbackArc, bufferSize)(Keep.none)

      val (pub, sub) = TestSource.probe[Long]
        .via(testedFlow)
        .toMat(TestSink.probe[Long])(Keep.both)
        .run()

      val N = 1000L

      sub.request(n = N)
      for (i <- 1L to N) {
        pub.sendNext(i)
      }

      sub.expectNextN(N)
    }

    "fail on too many messages in the feedback arc" in {
      val forwardFlow = PartitionWith((i: Long) => if (i > 0) Left(i - 1) else Right(i))
      val feedbackArc = Flow[Long].mapConcat(i => List(i, i))
      val testedFlow = FeedbackLoop(forwardFlow, feedbackArc, 1000)(Keep.none)

      val (pub, sub) = TestSource.probe[Long]
        .via(testedFlow)
        .toMat(TestSink.probe[Long])(Keep.both)
        .run()

      sub.request(n = 1)
      pub.sendNext(1000L)
      sub.expectError()
    }

    "fail on forward flow failure" in {
      import PartitionWith.Implicits._

      val forwardFlow = Flow[Long]
        .mapConcat(i => if (i > 0L) List(Left(i - 1), Right(i)) else throw new IllegalArgumentException(i.toString))
        .partitionWith(identity)
      val feedbackArc = Flow[Long]
      val testedFlow = FeedbackLoop(forwardFlow, feedbackArc, 1)(Keep.none)

      val (pub, sub) = TestSource.probe[Long]
        .via(testedFlow)
        .toMat(TestSink.probe[Long])(Keep.both)
        .run()

      sub.request(n = 10)
      pub.sendNext(5L)
      sub.expectNext(5L, 4L, 3L, 2L, 1L)
      val error = sub.expectError()
      assert(error.asInstanceOf[IllegalArgumentException].getMessage == 0L.toString)
    }

    "fail on feedback arc failure" in {
      import PartitionWith.Implicits._

      val forwardFlow = Flow[Long]
        .mapConcat(i => if (i > 0L) List(Left(i - 1), Right(i)) else List(Right(i)))
        .partitionWith(identity)
      val feedbackArc = Flow[Long].map(i => if (i > 2L) i else throw new IllegalArgumentException(i.toString))
      val testedFlow = FeedbackLoop(forwardFlow, feedbackArc, 1)(Keep.none)

      val (pub, sub) = TestSource.probe[Long]
        .via(testedFlow)
        .toMat(TestSink.probe[Long])(Keep.both)
        .run()

      sub.request(n = 10)
      pub.sendNext(5L)
      sub.expectNext(5L, 4L, 3L)
      val error = sub.expectError()
      assert(error.asInstanceOf[IllegalArgumentException].getMessage == 2L.toString)
    }

    "fail on upstream failure" in {
      import PartitionWith.Implicits._
      import FeedbackLoop.Implicits._

      val forwardFlow = Flow[Long]
        .mapConcat(i => if (i > 0L) List(Left(i - 1), Right(i)) else List(Right(i)))
        .partitionWith(identity)
      val testedFlow = forwardFlow.feedback(1)

      val (pub, sub) = TestSource.probe[Long]
        .via(testedFlow)
        .toMat(TestSink.probe[Long])(Keep.both)
        .run()

      sub.request(n = 10)
      pub.sendNext(5L)
      sub.expectNext(5L, 4L, 3L, 2L, 1L, 0L)
      pub.sendError(new IllegalArgumentException("foo"))
      val error = sub.expectError()
      assert(error.asInstanceOf[IllegalArgumentException].getMessage == "foo")
    }

    "keep working after feedback arc completes" in {
      import PartitionWith.Implicits._

      val forwardFlow = Flow[Long]
        .mapConcat { i => if (i > 0L) List(Left(i - 1), Right(i)) else List(Right(i)) }
        .partitionWith(identity)
      val feedbackArc = Flow[Long].take(5)
      val testedFlow = FeedbackLoop(forwardFlow, feedbackArc, 1)(Keep.none)

      val (pub, sub) = TestSource.probe[Long]
        .via(testedFlow)
        .toMat(TestSink.probe[Long])(Keep.both)
        .run()

      sub.request(n = 10)
      pub.sendNext(10L)
      sub.expectNext(10L, 9L, 8L, 7L, 6L, 5L)
      pub.sendNext(4L)
      sub.expectNext(4L)
      pub.sendNext(3L)
      sub.expectNext(3L)
    }

    "be able to compute the Fibonacci sequence" in {
      import PartitionWith.Implicits._
      import FeedbackLoop.Implicits._

      val forwardFlow = Flow[(Int, Int)]
        .mapConcat { case pair @ (n, _) => Right(n) :: Left(pair) :: Nil }
        .partitionWith(identity)
      val feedbackArc = Flow[(Int, Int)]
        .map { case (n, n1) => (n1, n + n1) }
      val testedFlow = forwardFlow.feedbackVia(feedbackArc, 1)

      val (pub, sub) = TestSource.probe[(Int, Int)]
        .via(testedFlow)
        .toMat(TestSink.probe[Int])(Keep.both)
        .run()

      sub.request(n = 20)
      pub.sendNext((0, 1))
      sub.expectNextN(List(0, 1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 987, 1597, 2584, 4181))
    }
  }
}
