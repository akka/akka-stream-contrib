/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.contrib

import akka.NotUsed
import akka.stream.scaladsl.{ Flow, Keep, Source }
import akka.stream.testkit.scaladsl.{ TestSink, TestSource }

class EitherViaSpec extends BaseStreamSpec {
  override protected def autoFusing: Boolean = true

  val rightFlow: Flow[Int, String, NotUsed] = Flow[Int].map(_.toString)
  val leftFlow: Flow[Throwable, String, NotUsed] = Flow[Throwable].map(_.getMessage)

  def asEither(i: Int): Either[Exception, Int] = if (i % 2 == 0) Right(i) else Left(new RuntimeException(s"ups, $i is odd"))

  "EitherFlow" should {
    "work with empty source" in {
      Source.empty[Int]
        .map(asEither)
        .via(EitherFlow.either(leftFlow, rightFlow))
        .runWith(TestSink.probe)
        .request(1)
        .expectComplete()
    }

    "direct to the right flow based on materialized value of Either" in {
      val (source, sink) = TestSource
        .probe[Int]
        .map(asEither)
        .via(EitherFlow.either(leftFlow, rightFlow))
        .toMat(TestSink.probe)(Keep.both)
        .run()

      sink.request(99)

      source.sendNext(1)
      source.sendNext(2)
      source.sendNext(3)

      sink.expectNext("ups, 1 is odd", "2", "ups, 3 is odd")

      source.sendComplete()
      sink.expectComplete()
    }
  }
}