package akka.stream.contrib

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl.{Flow, FlowOps, GraphDSL, RunnableGraph, Sink, Source}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}

import scala.annotation.unchecked.uncheckedVariance

/**
  * Created by Tal Pressman on 26.8.2016.
  *
  */
object FlowTagger {
  def tagged[In, Out, Tag, Mat](flow: Flow[In, Out, Mat]): Graph[FlowShape[(Tag, In), (Tag, Out)], Mat] =
    GraphDSL.create(flow) {
      implicit builder =>
        f =>
          import GraphDSL.Implicits._
          val drt = builder.add(new FlowTagger[In, Out, Tag])

          drt.out2 ~> f ~> drt.in2

          FlowShape(drt.in1, drt.out1)
    }

  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem()
    implicit val materializer = ActorMaterializer()

    val innerFlow = Flow[String].map(_.toUpperCase).filterNot(s => s.startsWith("O"))
    val foo = Source(List((1l, "one"), (2l, "two"), (3l, "three"), (4l, "four")))
      .via(tagged(innerFlow))
      .runWith(Sink.foreach(println))

    system.terminate()
  }
}

class FlowTagger[In, Out, E]
  extends GraphStage[BidiShape[(E, In), (E, Out), Out, In]] {
  val inTup = Inlet[(E, In)]("in-tuple")
  val outTup = Outlet[(E, Out)]("out-tuple")
  val toFlow = Outlet[In]("to-flow")
  val fromFlow = Inlet[Out]("from-flow")


  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    var curr: E = _
    setHandler(inTup, new InHandler {
      override def onPush(): Unit = {
        val (cur, in) = grab(inTup)
        curr = cur
        push(toFlow, in)
      }

      override def onUpstreamFinish(): Unit = complete(toFlow)

      override def onUpstreamFailure(ex: Throwable): Unit = fail(toFlow, ex)
    })

    setHandler(fromFlow, new InHandler {
      override def onPush(): Unit = {
        val out = grab(fromFlow)
        push(outTup, (curr, out))
      }
    })

    setHandler(outTup, new OutHandler {
      override def onDownstreamFinish(): Unit = cancel(fromFlow)

      override def onPull(): Unit = pull(fromFlow)
    })

    setHandler(toFlow, new OutHandler {
      override def onPull(): Unit = pull(inTup)
    })
  }

  override def shape = BidiShape(inTup, outTup, fromFlow, toFlow)
}

object FocusedFlow {
  implicit class FocusedFlowOps[Tag, Src, Mat](from: Flow[_, (Tag, Src), Mat]) {
    def focus = new FocusedFlow(from, Flow[Src])
  }

  implicit class FocusedSourceOps[Tag, Src, Mat](from: Source[(Tag, Src), Mat]) {
    def focus = new FocusedSource(from, Flow[Src])
  }

  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem()
    implicit val materializer = ActorMaterializer()

    val foo = Source(List((1l, "one"), (2l, "two"), (3l, "three"), (4l, "four")))
      .focus
        .map(_.toUpperCase)
        .filterNot(_.startsWith("T"))
      .unfocus
      .runWith(Sink.foreach(println))

    system.terminate()
  }
}

class FocusedFlow[Tag, SrcIn, SrcMat, -In, +Out](
   src: Flow[SrcIn, (Tag, In), SrcMat],
   innerFlow: Flow[In, Out, NotUsed]
   ) extends FlowOps[Out, NotUsed] {

  override type Repr[+O] = FocusedFlow[Tag, SrcIn, SrcMat, In@uncheckedVariance, O]
  override type Closed = Sink[SrcIn, SrcMat]

  def unfocus: Flow[SrcIn, (Tag, Out), SrcMat] =
    src.via(FlowTagger.tagged(innerFlow))

  override def via[T, Mat2](flow: Graph[FlowShape[Out, T], Mat2]): FocusedFlow[Tag, SrcIn, SrcMat, In, T] =
    new FocusedFlow(src, innerFlow.via(flow))

  // connecting the focused flow to a sink will discard the tag
  override def to[Mat2](sink: Graph[SinkShape[Out], Mat2]): Sink[SrcIn, SrcMat] =
    src.map(_._2).via(innerFlow).to(sink)

  override def withAttributes(attr: Attributes): FocusedFlow[Tag, SrcIn, SrcMat, In, Out] =
    new FocusedFlow(src, innerFlow.withAttributes(attr))

  override def addAttributes(attr: Attributes): FocusedFlow[Tag, SrcIn, SrcMat, In, Out] =
    new FocusedFlow(src, innerFlow.addAttributes(attr))

  override def named(name: String): FocusedFlow[Tag, SrcIn, SrcMat, In, Out] =
    new FocusedFlow(src, innerFlow.named(name))

  override def async: FocusedFlow[Tag, SrcIn, SrcMat, In, Out] =
    new FocusedFlow(src, innerFlow.async)
}

class FocusedSource[Tag, SrcMat, -In, +Out](
  src: Source[(Tag, In), SrcMat],
  innerFlow: Flow[In, Out, NotUsed]
  ) extends FlowOps[Out, NotUsed] {

  override type Repr[+O] = FocusedSource[Tag, SrcMat, In@uncheckedVariance, O]
  override type Closed = RunnableGraph[SrcMat]

  def unfocus: Source[(Tag, Out), SrcMat] =
    src.via(FlowTagger.tagged(innerFlow))

  override def via[T, Mat2](flow: Graph[FlowShape[Out, T], Mat2]): FocusedSource[Tag, SrcMat, In, T] =
    new FocusedSource(src, innerFlow.via(flow))

  // connecting the focused flow to a sink will discard the tag
  override def to[Mat2](sink: Graph[SinkShape[Out], Mat2]): RunnableGraph[SrcMat] =
    src.map(_._2).via(innerFlow).to(sink)

  override def withAttributes(attr: Attributes): FocusedSource[Tag, SrcMat, In, Out] =
    new FocusedSource(src, innerFlow.withAttributes(attr))

  override def addAttributes(attr: Attributes): FocusedSource[Tag, SrcMat, In, Out] =
    new FocusedSource(src, innerFlow.addAttributes(attr))

  override def named(name: String): FocusedSource[Tag, SrcMat, In, Out] =
    new FocusedSource(src, innerFlow.named(name))

  override def async: FocusedSource[Tag, SrcMat, In, Out] =
    new FocusedSource(src, innerFlow.async)
}
