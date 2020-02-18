/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.contrib

import java.time.Clock
import java.time.temporal.ChronoUnit

import akka.Done
import akka.annotation.InternalApi
import akka.stream._
import akka.stream.scaladsl.{Flow, GraphDSL, Sink}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.contrib.LatencyTimer._

import scala.concurrent.Future
import scala.concurrent.duration._

private final class LatencyTimerStartStage[InOut](private val clock: Clock) extends GraphStage[TimerStartShape[InOut]] {

  private[contrib] val in = Inlet.create[InOut]("LatencyTimerStart.In")
  private[contrib] val out = Outlet.create[InOut]("LatencyTimerStart.Out")
  private[contrib] val outTimerContext = Outlet.create[TimerContext]("LatencyTimerStart.OutTimerContext")

  override val shape: TimerStartShape[InOut] = TimerStartShape(in, out, outTimerContext)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {

    setHandler(in, new InHandler {
      override def onPush(): Unit = {
        emit(outTimerContext, TimerContext(clock))
        push(out, grab(in))
      }
    })

    private[this] val outHandler = new OutHandler {
      override def onPull(): Unit = if (!hasBeenPulled(in)) tryPull(in)
    }

    setHandler(out, outHandler)
    setHandler(outTimerContext, outHandler)
  }
}

private final class LatencyTimerEndStage[InOut] extends GraphStage[TimerEndShape[InOut]] {

  private[contrib] val in = Inlet.create[InOut]("LatencyTimerEnd.In")
  private[contrib] val inTimerContext = Inlet.create[TimerContext]("LatencyTimerEnd.InTimerContext")
  private[contrib] val out = Outlet.create[InOut]("LatencyTimerEnd.Out")
  private[contrib] val outTimedResult = Outlet.create[TimedResult[InOut]]("LatencyTimerEnd.TimedResult")

  override val shape: TimerEndShape[InOut] = TimerEndShape(in, inTimerContext, out, outTimedResult)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {

    private[this] var timerContext = None: Option[TimerContext]

    setHandler(
      in,
      new InHandler {
        override def onPush(): Unit = {
          val element = grab(in)
          emit(outTimedResult, TimedResult(element, timerContext.get.stop()))
          timerContext = None
          push(out, element)
        }
      }
    )

    setHandler(inTimerContext, new InHandler {
      override def onPush(): Unit = timerContext = Some(grab(inTimerContext))
    })

    setHandler(out, new OutHandler {
      override def onPull(): Unit = tryPull(in)
    })

    setHandler(outTimedResult, new OutHandler {
      override def onPull(): Unit = tryPull(inTimerContext)
    })
  }
}

/**
 * {{{
 *            +------------+
 *            |            | ~> InOut
 *   InOut ~> | TimerStart |
 *            |            | ~> TimerContext
 *            +------------+
 * }}}
 */
private final case class TimerStartShape[InOut](in: Inlet[InOut],
                                                out: Outlet[InOut],
                                                outTimerContext: Outlet[TimerContext])
    extends Shape {
  override val inlets: Seq[Inlet[_]] = in :: Nil
  override val outlets: Seq[Outlet[_]] = out :: outTimerContext :: Nil

  override def deepCopy(): TimerStartShape[InOut] =
    TimerStartShape(in.carbonCopy(), out.carbonCopy(), outTimerContext.carbonCopy())
}

/**
 * {{{
 *                   +------------+
 *          InOut ~> |            | ~> InOut
 *                   |  TimerEnd  |
 *   TimerContext ~> |            | ~> TimedResult[InOut, FiniteDuration]
 *                   +------------+
 * }}}
 */
private final case class TimerEndShape[InOut](in: Inlet[InOut],
                                              inTimerContext: Inlet[TimerContext],
                                              out: Outlet[InOut],
                                              outTimedResult: Outlet[TimedResult[InOut]])
    extends Shape {
  override val inlets: Seq[Inlet[_]] = in :: inTimerContext :: Nil
  override val outlets: Seq[Outlet[_]] = out :: outTimedResult :: Nil

  override def deepCopy(): TimerEndShape[InOut] =
    TimerEndShape(in.carbonCopy(), inTimerContext.carbonCopy(), out.carbonCopy(), outTimedResult.carbonCopy())
}

private[contrib] object LatencyTimerStartStage {
  def apply[T](clock: Clock): LatencyTimerStartStage[T] = new LatencyTimerStartStage[T](clock)
}

private[contrib] object LatencyTimerEndStage {
  def apply[T](): LatencyTimerEndStage[T] = new LatencyTimerEndStage[T]()
}

object LatencyTimer {

  private[contrib] case class TimerContext(clock: Clock) {
    private val started = clock.instant()

    def stop(): FiniteDuration = FiniteDuration(started.until(clock.instant(), ChronoUnit.NANOS), NANOSECONDS)
  }

  case class TimedResult[T](outcome: T, measuredTime: FiniteDuration)

  @InternalApi // mainly for testing (mock Clock)
  private[contrib] def createGraph[I, O, Mat](
      flow: Flow[I, O, Mat],
      sink: Graph[SinkShape[TimedResult[O]], Future[Done]]
  )(clock: Clock): Flow[I, O, Mat] = {
    val graph = GraphDSL.create(flow) { implicit builder: GraphDSL.Builder[Mat] =>
      import GraphDSL.Implicits._
      fl =>
        // Junctions need to be created from blueprint via `builder.add(...)`
        val startTimer = builder.add(LatencyTimerStartStage[I](clock))
        val endTimer = builder.add(LatencyTimerEndStage[O]())

        // @formatter:off
        startTimer.out              ~> fl ~> endTimer.in
        startTimer.outTimerContext  ~>       endTimer.inTimerContext; endTimer.outTimedResult ~> sink
        // @formatter:on
        FlowShape(startTimer.in, endTimer.out)
    }
    Flow.fromGraph(graph)
  }

  /**
   * Wraps a given flow and measures the time between input and output. The measured result is pushed to a dedicated sink.
   * The [[TimedResult]] contains the result of the wrapped flow as well in case some logic has to be done.
   *
   * Important Note: the wrapped flow must preserve the order, otherwise timing will be wrong.
   *
   * Consider bringing [[akka.stream.contrib.Implicits]] into scope for DSL support.
   *
   * @param flow the flow which will be measured
   * @param sink a sink which will handle the [[TimedResult]]
   * @tparam I   input-type of the wrapped flow
   * @tparam O   output-type of the wrapped flow
   * @tparam Mat materialized-type of the wrapped flow
   * @return Flow of the the same shape as the wrapped flow
   */
  def apply[I, O, Mat](flow: Flow[I, O, Mat], sink: Graph[SinkShape[TimedResult[O]], Future[Done]]): Flow[I, O, Mat] =
    createGraph(flow, sink)(Clock.systemDefaultZone())

  /**
   * Wraps a given flow and measures the time between input and output. The second parameter is the function which is called for each measured element.
   * The [[TimedResult]] contains the result of the wrapped flow as well in case some logic has to be done.
   *
   * Important Note: the wrapped flow must preserve the order, otherwise timing will be wrong.
   *
   * Consider bringing [[akka.stream.contrib.Implicits]] into scope for DSL support.
   *
   * @param flow           the flow which will be measured
   * @param resultFunction side-effect function which gets called with the result
   * @tparam I   input-type of the wrapped flow
   * @tparam O   output-type of the wrapped flow
   * @tparam Mat materialized-type of the wrapped flow
   * @return Flow of the the same shape as the wrapped flow
   */
  def apply[I, O, Mat](flow: Flow[I, O, Mat], resultFunction: TimedResult[O] => Unit): Flow[I, O, Mat] =
    this(flow, Sink.foreach[TimedResult[O]](resultFunction))
}
