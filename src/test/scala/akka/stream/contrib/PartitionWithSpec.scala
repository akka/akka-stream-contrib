/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.contrib

import akka.NotUsed
import akka.stream.scaladsl._
import akka.stream.{ClosedShape, FanInShape2, FanOutShape2, FlowShape, Graph}
import akka.stream.testkit.scaladsl.{TestSink, TestSource}

class PartitionWithSpec extends BaseStreamSpec {

  private def fanOutAndIn[I, X, Y, O, M](fanOutGraph: Graph[FanOutShape2[I, X, Y], M],
                                         fanInGraph: Graph[FanInShape2[X, Y, O], NotUsed]): Flow[I, O, M] =
    Flow.fromGraph(GraphDSL.create(fanOutGraph, fanInGraph)(Keep.left) { implicit builder => (fanOut, fanIn) =>
      import GraphDSL.Implicits._

      fanOut.out0 ~> fanIn.in0
      fanOut.out1 ~> fanIn.in1

      FlowShape(fanOut.in, fanIn.out)
    })

  private def zipFanOut[I, O1, O2, M](fanOutGraph: Graph[FanOutShape2[I, O1, O2], M]): Flow[I, (O1, O2), M] =
    fanOutAndIn(fanOutGraph, Zip[O1, O2])

  private def mergeFanOut[I, O, M](fanOutGraph: Graph[FanOutShape2[I, O, O], M]): Flow[I, O, M] =
    Flow.fromGraph(GraphDSL.create(fanOutGraph) { implicit builder => fanOut =>
      import GraphDSL.Implicits._

      val mrg = builder.add(Merge[O](2))

      fanOut.out0 ~> mrg.in(0)
      fanOut.out1 ~> mrg.in(1)

      FlowShape(fanOut.in, mrg.out)
    })

  val flow = mergeFanOut(PartitionWith[Int, Int, Int] {
    case i if i % 2 == 0 => Left(i / 2)
    case i => Right(i * 3 - 1)
  })

  "PartitionWith" should {
    "partition ints according to their parity" in {

      val (source, sink) = TestSource
        .probe[Int]
        .via(flow)
        .toMat(TestSink.probe)(Keep.both)
        .run()
      sink.request(99)
      source.sendNext(1)
      source.sendNext(2)
      source.sendNext(3)
      sink.expectNext(2, 1, 8)
      source.sendComplete()
      sink.expectComplete()
    }

    "not emit any value for an empty source" in {
      Source(Vector.empty[Int])
        .via(flow)
        .runWith(TestSink.probe)
        .request(99)
        .expectComplete()
    }

    "fail on upstream failure" in {
      val (source, sink) = TestSource
        .probe[Int]
        .via(flow)
        .toMat(TestSink.probe)(Keep.both)
        .run()
      sink.request(99)
      source.sendError(new Exception)
      sink.expectError()
    }

    "allow flow of values of one partition even when the other outlet was not pulled" in {
      val source = TestSource.probe[Int]
      val sink0 = TestSink.probe[Int]
      val sink1 = TestSink.probe[Int]

      val graph = GraphDSL.create(source, sink0, sink1)(Tuple3.apply) { implicit b => (src, snk0, snk1) =>
        import GraphDSL.Implicits._

        val pw = b.add(PartitionWith[Int, Int, Int] {
          case i if i % 2 == 0 => Left(i)
          case i => Right(i)
        })

        src.out ~> pw.in
        pw.out0 ~> snk0.in
        pw.out1 ~> snk1.in

        ClosedShape
      }
      val (pub, sub1, sub2) = RunnableGraph.fromGraph(graph).run()

      sub1.request(10)
      (1 to 10).foreach(i => pub.sendNext(2 * i))
      sub1.expectNext(2, 4, 6, 8, 10, 12, 14, 16, 18, 20)

      sub2.request(10)
      (1 to 10).foreach(i => pub.sendNext(2 * i + 1))
      sub2.expectNext(3, 5, 7, 9, 11, 13, 15, 17, 19, 21)
    }

    "with eagerCancel=false (the default), continue after cancellation of one of the downstreams" in {
      val source = TestSource.probe[Int]
      val sink0 = TestSink.probe[Int]
      val sink1 = TestSink.probe[Int]

      val graph = GraphDSL.create(source, sink0, sink1)(Tuple3.apply) { implicit b => (src, snk0, snk1) =>
        import GraphDSL.Implicits._

        val partition = b.add(PartitionWith[Int, Int, Int](i => if (i % 2 == 0) Left(i) else Right(i)))

        src.out ~> partition.in
        partition.out0 ~> snk0.in
        partition.out1 ~> snk1.in

        ClosedShape
      }
      val (pub, sub0, sub1) = RunnableGraph.fromGraph(graph).run()

      sub1.request(n = 1)
      sub0.cancel()
      pub.sendNext(5)
      sub1.expectNext(5)
    }

    "with eagerCancel=true, cancel and complete the other downstream after cancellation of one of the downstreams" in {
      val source = TestSource.probe[Int]
      val sink0 = TestSink.probe[Int]
      val sink1 = TestSink.probe[Int]

      val graph = GraphDSL.create(source, sink0, sink1)(Tuple3.apply) { implicit b => (src, snk0, snk1) =>
        import GraphDSL.Implicits._

        val partition =
          b.add(PartitionWith[Int, Int, Int](i => if (i % 2 == 0) Left(i) else Right(i), eagerCancel = true))

        src.out ~> partition.in
        partition.out0 ~> snk0.in
        partition.out1 ~> snk1.in

        ClosedShape
      }
      val (pub, sub0, sub1) = RunnableGraph.fromGraph(graph).run()

      sub1.request(n = 1)
      sub0.cancel()
      pub.expectCancellation()
      sub1.expectComplete()
    }
  }

  "partitionWith extension method" should {
    "be callable on Flow and partition its output" in {
      import PartitionWith.Implicits._

      val flow = zipFanOut(Flow[Int].partitionWith(i => if (i % 2 == 0) Left(-i) else Right(i)))

      val (source, sink) = TestSource
        .probe[Int]
        .via(flow)
        .toMat(TestSink.probe)(Keep.both)
        .run()

      sink.request(5)
      (1 to 10).foreach(source.sendNext)
      sink.expectNextN(List((-2, 1), (-4, 3), (-6, 5), (-8, 7), (-10, 9)))
    }
  }
}
