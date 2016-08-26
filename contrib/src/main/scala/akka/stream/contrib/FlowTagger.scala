package akka.stream.contrib

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl.{Flow, FlowOpsMat, GraphDSL, Keep, RunnableGraph, Sink, Source}
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
        .filterNot(_.startsWith("O"))
      .unfocus
      .runWith(Sink.foreach(println))

    system.terminate()
  }
}

class FocusedFlow[Tag, SrcIn, SrcMat, -In, +Out, +Mat](
   src: Flow[SrcIn, (Tag, In), SrcMat],
   innerFlow: Flow[In, Out, Mat]
   ) extends FlowOpsMat[Out, Mat] {

  override type Repr[+O] = FocusedFlow[Tag, SrcIn, SrcMat, In@uncheckedVariance, O, Mat@uncheckedVariance]
  override type ReprMat[+O, +M] = FocusedFlow[Tag, SrcIn, SrcMat, In@uncheckedVariance, O, M]
  override type Closed = Sink[SrcIn, SrcMat]
  override type ClosedMat[+M] = Sink[SrcIn@uncheckedVariance, M]

  def unfocus: Flow[SrcIn, (Tag, Out), SrcMat] =
    src.via(FlowTagger.tagged(innerFlow))

  def unfocusMat[Mat2](combine: (SrcMat, Mat) => Mat2): Flow[SrcIn, (Tag, Out), Mat2] =
    src.viaMat(FlowTagger.tagged(innerFlow))(combine)

  override def via[T, Mat2](flow: Graph[FlowShape[Out, T], Mat2]): FocusedFlow[Tag, SrcIn, SrcMat, In, T, Mat] =
    viaMat(flow)(Keep.left)
  override def viaMat[T, Mat2, Mat3](flow: Graph[FlowShape[Out, T], Mat2])(combine: (Mat, Mat2) => Mat3): FocusedFlow[Tag, SrcIn, SrcMat, In, T, Mat3] =
    new FocusedFlow(src, innerFlow.viaMat(flow)(combine))

  // connecting the focused flow to a sink will discard the tag and the original materialized value!
  override def to[Mat2](sink: Graph[SinkShape[Out], Mat2]): Sink[SrcIn, SrcMat] =
    src.map(_._2).via(innerFlow).to(sink)
  override def toMat[Mat2, Mat3](sink: Graph[SinkShape[Out], Mat2])(combine: (Mat, Mat2) => Mat3): Sink[SrcIn, Mat3] =
    src.map(_._2).toMat(innerFlow.toMat(sink)(combine))(Keep.right)

  override def mapMaterializedValue[Mat2](f: (Mat) => Mat2): FocusedFlow[Tag, SrcIn, SrcMat, In, Out, Mat2] =
    new FocusedFlow(src, innerFlow.mapMaterializedValue(f))

  override def withAttributes(attr: Attributes): FocusedFlow[Tag, SrcIn, SrcMat, In, Out, Mat] =
    new FocusedFlow(src, innerFlow.withAttributes(attr))

  override def addAttributes(attr: Attributes): FocusedFlow[Tag, SrcIn, SrcMat, In, Out, Mat] =
    new FocusedFlow(src, innerFlow.addAttributes(attr))

  override def named(name: String): FocusedFlow[Tag, SrcIn, SrcMat, In, Out, Mat] =
    new FocusedFlow(src, innerFlow.named(name))

  override def async: FocusedFlow[Tag, SrcIn, SrcMat, In, Out, Mat] =
    new FocusedFlow(src, innerFlow.async)
}

class FocusedSource[Tag, SrcMat, -In, +Out, +Mat](
  src: Source[(Tag, In), SrcMat],
  innerFlow: Flow[In, Out, Mat]
  ) extends FlowOpsMat[Out, Mat] {

  override type Repr[+O] = FocusedSource[Tag, SrcMat, In@uncheckedVariance, O, Mat@uncheckedVariance]
  override type ReprMat[+O, +M] = FocusedSource[Tag, SrcMat, In@uncheckedVariance, O, M]
  override type Closed = RunnableGraph[SrcMat]
  override type ClosedMat[+M] = RunnableGraph[M]

  def unfocus: Source[(Tag, Out), SrcMat] =
    src.via(FlowTagger.tagged(innerFlow))

  def unfocusMat[Mat2](combine: (SrcMat, Mat) => Mat2): Source[(Tag, Out), Mat2] =
    src.viaMat(FlowTagger.tagged(innerFlow))(combine)

  override def via[T, Mat2](flow: Graph[FlowShape[Out, T], Mat2]): FocusedSource[Tag, SrcMat, In, T, Mat] =
    viaMat(flow)(Keep.left)
  override def viaMat[T, Mat2, Mat3](flow: Graph[FlowShape[Out, T], Mat2])(combine: (Mat, Mat2) => Mat3): FocusedSource[Tag, SrcMat, In, T, Mat3] =
    new FocusedSource(src, innerFlow.viaMat(flow)(combine))

  // connecting the focused flow to a sink will discard the tag and the original materialized value!
  override def to[Mat2](sink: Graph[SinkShape[Out], Mat2]): RunnableGraph[SrcMat] =
    src.map(_._2).via(innerFlow).to(sink)
  override def toMat[Mat2, Mat3](sink: Graph[SinkShape[Out], Mat2])(combine: (Mat, Mat2) => Mat3): RunnableGraph[Mat3] =
    src.map(_._2).toMat(innerFlow.toMat(sink)(combine))(Keep.right)

  override def mapMaterializedValue[Mat2](f: (Mat) => Mat2): FocusedSource[Tag, SrcMat, In, Out, Mat2] =
    new FocusedSource(src, innerFlow.mapMaterializedValue(f))

  override def withAttributes(attr: Attributes): FocusedSource[Tag, SrcMat, In, Out, Mat] =
    new FocusedSource(src, innerFlow.withAttributes(attr))

  override def addAttributes(attr: Attributes): FocusedSource[Tag, SrcMat, In, Out, Mat] =
    new FocusedSource(src, innerFlow.addAttributes(attr))

  override def named(name: String): FocusedSource[Tag, SrcMat, In, Out, Mat] =
    new FocusedSource(src, innerFlow.named(name))

  override def async: FocusedSource[Tag, SrcMat, In, Out, Mat] =
    new FocusedSource(src, innerFlow.async)
}
