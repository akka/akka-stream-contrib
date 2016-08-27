package akka.stream.contrib

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.contrib.FocusedSource.IncompleteFocusedSourceShape
import akka.stream.scaladsl.{BidiFlow, Flow, FlowOpsMat, GraphDSL, Keep, RunnableGraph, Sink, Source}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}

import scala.annotation.unchecked.uncheckedVariance
import scala.collection.immutable

object FocusedFlow {
  implicit class FocusedFlowOps[Tag, Src, Mat](from: Flow[_, (Tag, Src), Mat]) {
    def focus = new FocusedFlow(BidiFlow.fromFlowsMat(from, Flow[Src])(Keep.left))
  }
}

class FocusedFlow[Tag, -SrcIn, In, +Out, +Mat] private (
   graph: Graph[BidiShape[SrcIn, (Tag, In), In, Out], Mat]
  ) extends FlowOpsMat[Out, Mat] {
  import GraphDSL.Implicits._

  override type Repr[+O] = FocusedFlow[Tag, SrcIn@uncheckedVariance, In, O, Mat@uncheckedVariance]
  override type ReprMat[+O, +M] = FocusedFlow[Tag, SrcIn@uncheckedVariance, In, O, M]
  override type Closed = Sink[SrcIn@uncheckedVariance, Mat@uncheckedVariance]
  override type ClosedMat[+M] = Sink[SrcIn@uncheckedVariance, M]

  def unfocus: Flow[SrcIn, (Tag, Out), Mat] = Flow.fromGraph(
    GraphDSL.create(graph) {
      implicit builder =>
      g =>
        val tagger = builder.add(new FlowTagger[In, Out, Tag])

        g.out1 ~> tagger.in1
        tagger.out2 ~> g.in2
        g.out2 ~> tagger.in2

        FlowShape(g.in1, tagger.out1)
    })

  override def via[T, Mat2](flow: Graph[FlowShape[Out, T], Mat2]): FocusedFlow[Tag, SrcIn, In, T, Mat] =
    viaMat(flow)(Keep.left)
  override def viaMat[T, Mat2, Mat3](flow: Graph[FlowShape[Out, T], Mat2])(combine: (Mat, Mat2) => Mat3): FocusedFlow[Tag, SrcIn, In, T, Mat3] =
    new FocusedFlow(GraphDSL.create(graph, flow)(combine) {
      implicit builder =>
      (g, f) =>
        g.out2 ~> f
        BidiShape(g.in1, g.out1, g.in2, f.out)
    })

  // connecting the focused flow to a sink will discard the tag
  override def to[Mat2](sink: Graph[SinkShape[Out], Mat2]): Sink[SrcIn, Mat] =
    toMat(sink)(Keep.left)
  override def toMat[Mat2, Mat3](sink: Graph[SinkShape[Out], Mat2])(combine: (Mat, Mat2) => Mat3): Sink[SrcIn, Mat3] =
    Sink.fromGraph(
      GraphDSL.create(graph, sink)(combine) {
        implicit builder =>
        (g, s) =>
          val mapper = builder.add(new akka.stream.impl.fusing.Map[(_, In), In](_._2))
          g.out1 ~> mapper ~> g.in2
          g.out2 ~> s.in
          SinkShape(g.in1)
      })

  override def mapMaterializedValue[Mat2](f: (Mat) => Mat2): FocusedFlow[Tag, SrcIn, In, Out, Mat2] =
    viaMat(Flow[Out]) { case (m, notused) => f(m) }

  override def withAttributes(attr: Attributes): FocusedFlow[Tag, SrcIn, In, Out, Mat] =
    new FocusedFlow(graph.withAttributes(attr))

  override def addAttributes(attr: Attributes): FocusedFlow[Tag, SrcIn, In, Out, Mat] =
    new FocusedFlow(graph.addAttributes(attr))

  override def named(name: String): FocusedFlow[Tag, SrcIn, In, Out, Mat] =
    new FocusedFlow(graph.named(name))

  override def async: FocusedFlow[Tag, SrcIn, In, Out, Mat] =
    new FocusedFlow(graph.async)
}

private object FocusedSource {
  implicit class FocusedSourceOps[Tag, Src, Mat](from: Source[(Tag, Src), Mat]) {
    def focus = new FocusedSource(GraphDSL.create(from, Flow[Src])(Keep.left) {
      implicit builder =>
        (src, id) =>
          IncompleteFocusedSourceShape(src.out, id.in, id.out)
    })
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

  private final case class IncompleteFocusedSourceShape[+SrcIn, In, +Out](
   srcIn: Outlet[SrcIn@uncheckedVariance],
   in: Inlet[In@uncheckedVariance],
   out: Outlet[Out@uncheckedVariance]
  ) extends Shape {
    override val inlets: immutable.Seq[Inlet[_]] = List(in)
    override val outlets: immutable.Seq[Outlet[_]] = List(srcIn, out)

    override def deepCopy(): IncompleteFocusedSourceShape[SrcIn, In, Out] =
      IncompleteFocusedSourceShape(srcIn.carbonCopy(), in.carbonCopy(), out.carbonCopy())
    override def copyFromPorts(inlets: immutable.Seq[Inlet[_]], outlets: immutable.Seq[Outlet[_]]): Shape = {
      require(inlets.size == 1, s"proposed inlets [${inlets.mkString(", ")}] do not fit IncompleteFocusedSourceShape")
      require(outlets.size == 2, s"proposed outlets [${outlets.mkString(", ")}] do not fit IncompleteFocusedSourceShape")
      IncompleteFocusedSourceShape(outlets(0), inlets(0), outlets(1))
    }

  }
}

class FocusedSource[Tag, In, +Out, +Mat] private (
   graph: Graph[IncompleteFocusedSourceShape[(Tag, In), In, Out], Mat]
  ) extends FlowOpsMat[Out, Mat] {
  import GraphDSL.Implicits._

  override type Repr[+O] = FocusedSource[Tag, In, O, Mat@uncheckedVariance]
  override type ReprMat[+O, +M] = FocusedSource[Tag, In, O, M]
  override type Closed = RunnableGraph[Mat@uncheckedVariance]
  override type ClosedMat[+M] = RunnableGraph[M]

  def unfocus: Source[(Tag, Out), Mat] = Source.fromGraph(
    GraphDSL.create(graph) {
      implicit builder =>
      g =>
        val tagger = builder.add(new FlowTagger[In, Out, Tag])

        g.srcIn ~> tagger.in1
        tagger.out2 ~> g.in
        g.out ~> tagger.in2

        SourceShape(tagger.out1)
    })

  override def via[T, Mat2](flow: Graph[FlowShape[Out, T], Mat2]): FocusedSource[Tag, In, T, Mat] =
    viaMat(flow)(Keep.left)
  override def viaMat[T, Mat2, Mat3](flow: Graph[FlowShape[Out, T], Mat2])(combine: (Mat, Mat2) => Mat3): FocusedSource[Tag, In, T, Mat3] =
    new FocusedSource(GraphDSL.create(graph, flow)(combine) {
      implicit builder =>
        (g, f) =>
          g.out ~> f
          IncompleteFocusedSourceShape(g.srcIn, g.in, f.out)
    })

  // connecting the focused flow to a sink will discard the tag
  override def to[Mat2](sink: Graph[SinkShape[Out], Mat2]): RunnableGraph[Mat] =
  toMat(sink)(Keep.left)
  override def toMat[Mat2, Mat3](sink: Graph[SinkShape[Out], Mat2])(combine: (Mat, Mat2) => Mat3): RunnableGraph[Mat3] =
    RunnableGraph.fromGraph(
      GraphDSL.create(graph, sink)(combine) {
        implicit builder =>
          (g, s) =>
            val mapper = builder.add(new akka.stream.impl.fusing.Map[(_, In), In](_._2))
            g.srcIn ~> mapper ~> g.in
            g.out ~> s.in
            ClosedShape
      })

  override def mapMaterializedValue[Mat2](f: (Mat) => Mat2): FocusedSource[Tag, In, Out, Mat2] =
    viaMat(Flow[Out]) { case (m, notused) => f(m) }

  override def withAttributes(attr: Attributes): FocusedSource[Tag, In, Out, Mat] =
    new FocusedSource(graph.withAttributes(attr))

  override def addAttributes(attr: Attributes): FocusedSource[Tag, In, Out, Mat] =
    new FocusedSource(graph.addAttributes(attr))

  override def named(name: String): FocusedSource[Tag, In, Out, Mat] =
    new FocusedSource(graph.named(name))

  override def async: FocusedSource[Tag, In, Out, Mat] =
    new FocusedSource(graph.async)
}

private class FlowTagger[In, Out, E]
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
