/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.contrib

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, GraphDSL, Keep, Source, Zip}
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.stream.{ActorMaterializer, FlowShape}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{EitherValues, Matchers, WordSpecLike}
import scala.concurrent.duration._

class BufferedBroadcastSpec extends WordSpecLike with Matchers with ScalaFutures with EitherValues {

  implicit val as: ActorSystem = ActorSystem("test")
  implicit val materializer: ActorMaterializer = ActorMaterializer()

  "BufferedBroadcastStage" should {
    "broadcast elements even when there is pull only from one output" in {
      val f = Flow.fromGraph(GraphDSL.create() { implicit builder =>
        import GraphDSL.Implicits._

        val zip = builder.add(Zip[Int, Int])

        val broadcast = builder.add(BufferedBroadcast[Int](2))

        val left = broadcast.out(0).filter(_ <= 3)
        val right = broadcast.out(1).filter(_ > 3).concat(Source.repeat(0))

        left ~> zip.in0
        right ~> zip.in1

        FlowShape(broadcast.in, zip.out)
      })

      val (source, sink) = TestSource
        .probe[Int]
        .via(f)
        .toMat(TestSink.probe)(Keep.both)
        .run()

      source.sendNext(0)
      sink.request(10)
      sink.expectNoMessage(100.millis)
      source.sendNext(1)
      source.sendNext(3)
      sink.expectNoMessage(100.millis)
      source.sendNext(4)
      sink.expectNext((0, 4))
      source.sendNext(4)
      sink.expectNext((1, 4))
      sink.expectNoMessage(100.millis)
      source.sendNext(4)
      sink.expectNext((3, 4))
      source.sendNext(4)
      sink.expectNoMessage(100.millis)

      source.sendComplete()
      sink.expectComplete()
    }
  }
}
