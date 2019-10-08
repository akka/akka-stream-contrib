package akka.stream.contrib

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, GraphDSL, Partition, RunnableGraph, Sink, Source}
import akka.stream.testkit.{TestPublisher, TestSubscriber}
import akka.stream.{ActorMaterializer, ClosedShape, FlowShape, OverflowStrategy}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{MustMatchers, WordSpec}

import scala.concurrent.duration._
import scala.util.Random

//noinspection TypeAnnotation
class MergeByIndexSpec extends WordSpec with MustMatchers with ScalaFutures {
  implicit val system = ActorSystem("merge-by-index")
  implicit val mat = ActorMaterializer()
  implicit override val patienceConfig = PatienceConfig(5.seconds)

  "MergeByIndex" should {

    "pull from all upstreams" in new ThreeWayMergeGraph {
      // expecting subscriptions is already performed in ThreeWayMergeGraph
      completeAll()
    }

    "merge elements in order" in new ThreeWayMergeGraph {
      in1.sendNext(1L)
      in2.sendNext(0L)
      requestedNextOne() mustBe 0L
      requestedNextOne() mustBe 1L
      completeAll()
    }

    "wait for missing index if not all inputs are available" in new ThreeWayMergeGraph {
      in1.sendNext(1L)
      noNextOne() // waits for index 0.

      in3.sendNext(2L)
      out.expectNoMessage(50.millis) // still waits for index 0.

      in2.sendNext(0L)
      nextOne() mustBe 0L // emits index 0.
      requestedNextOne() mustBe 1L // emits index 1.
      requestedNextOne() mustBe 2L // emits index 2.
      completeAll()
    }

    "emit element on index omission" in new ThreeWayMergeGraph {
      in1.sendNext(1L)
      noNextOne() // waits for index 0.

      in2.sendNext(2L)
      in3.sendNext(3L)
      nextOne() mustBe 1L // emits index 1 as we now know there's a gap.
      completeAll()
    }

    "complete only when all inputs complete" in new ThreeWayMergeGraph {
      in3.sendNext(1L)
      in2.sendNext(0L)
      in1.sendNext(2L)

      requestedNextOne()
      requestedNextOne()
      requestedNextOne()

      in2.sendComplete()
      out.expectNoMessage(50.millis)

      in1.sendComplete()
      out.expectNoMessage(50.millis)

      in3.sendComplete()
      out.expectComplete()
    }

    "error if index sequence is non-monotonic" in new ThreeWayMergeGraph {
      in1.sendNext(1L)
      in2.sendNext(0L)
      requestedNextOne() mustBe 0L
      requestedNextOne() mustBe 1L

      in3.sendNext(1L)
      out.expectError() mustBe a[IllegalArgumentException]
    }

    "emit element on completion after index omission" in new ThreeWayMergeGraph {
      in1.sendNext(2L)
      in2.sendNext(1L) // index 0 is omitted
      in3.sendComplete() // ... which the stage should now infer due to completion of in3

      requestedNextOne() mustBe 1L
      requestedNextOne() mustBe 2L
      in2.sendComplete()
      in1.sendComplete()

      out.expectComplete()
    }

    "always merge items in order" in {
      // using poor man's property-based testing
      val inputLength = 1000
      val testRepetitions = 50
      val branchCount = 20

      val flow = Flow.fromGraph(GraphDSL.create() { implicit builder: GraphDSL.Builder[NotUsed] =>
        import GraphDSL.Implicits._

        val partition = builder.add(Partition[Long](branchCount, _ => Random.between(0, branchCount)))
        val merge = builder.add(MergeByIndex(branchCount, identity[Long]))
        val buffer = Flow[Long].buffer(3 * inputLength / branchCount, OverflowStrategy.backpressure)

        for (_ <- 1 to branchCount) partition ~> buffer ~> merge

        FlowShape(partition.in, merge.out)
      })

      for (_ <- 1 to testRepetitions) {
        val input = List.tabulate(inputLength)(_.toLong).filterNot(_ => Random.between(0, 5) == 0) // add random gaps
        val output = Source(input).via(flow).runWith(Sink.seq).futureValue
        output mustBe input
      }
    }
  }

  trait ThreeWayMergeGraph {
    val pub1 = TestPublisher.manualProbe[Long]()
    val pub2 = TestPublisher.manualProbe[Long]()
    val pub3 = TestPublisher.manualProbe[Long]()
    val out = TestSubscriber.manualProbe[Long]()

    val graph = RunnableGraph.fromGraph(GraphDSL.create() { implicit builder: GraphDSL.Builder[NotUsed] =>
      import GraphDSL.Implicits._
      val in1 = Source.fromPublisher(pub1)
      val in2 = Source.fromPublisher(pub2)
      val in3 = Source.fromPublisher(pub3)
      val outSink = Sink.fromSubscriber(out)

      val merge = builder.add(MergeByIndex(3, identity[Long]))

      in1 ~> merge ~> outSink
      in2 ~> merge
      in3 ~> merge
      ClosedShape
    })

    graph.run()

    val in1 = pub1.expectSubscription()
    val in2 = pub2.expectSubscription()
    val in3 = pub3.expectSubscription()

    val subscription = out.expectSubscription()

    def requestedNextOne(): Long = {
      subscription.request(1)
      out.expectNext()
    }

    def nextOne(): Long = out.expectNext()

    def noNextOne(): Unit = {
      subscription.request(1)
      out.expectNoMessage(50.millis)
    }

    def completeAll(): Unit = {
      in1.sendComplete()
      in2.sendComplete()
      in3.sendComplete()
    }
  }
}
