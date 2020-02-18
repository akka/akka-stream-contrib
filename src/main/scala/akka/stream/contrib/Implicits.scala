/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.contrib

import akka.Done
import akka.stream.contrib.LatencyTimer.TimedResult
import akka.stream.{Graph, SinkShape}
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Source

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

/**
 * Additional [[akka.stream.scaladsl.Flow]] and [[akka.stream.scaladsl.Flow]] operators.
 */
object Implicits {

  /**
   * Provides time measurement utilities on Stream elements.
   *
   * See [[Timed]]
   */
  implicit class TimedSourceDsl[I, Mat](val source: Source[I, Mat]) extends AnyVal {

    /**
     * Measures time from receiving the first element and completion events - one for each subscriber of this `Flow`.
     */
    def timed[O, Mat2](measuredOps: Source[I, Mat] ⇒ Source[O, Mat2],
                       onComplete: FiniteDuration ⇒ Unit): Source[O, Mat2] =
      Timed.timed[I, O, Mat, Mat2](source, measuredOps, onComplete)

    /**
     * Measures rolling interval between immediately subsequent `matching(o: O)` elements.
     */
    def timedIntervalBetween(matching: I ⇒ Boolean, onInterval: FiniteDuration ⇒ Unit): Source[I, Mat] =
      Timed.timedIntervalBetween[I, Mat](source, matching, onInterval)
  }

  /**
   * Provides time measurement utilities on Stream elements.
   *
   * See [[Timed]]
   */
  implicit class TimedFlowDsl[I, O, Mat](val flow: Flow[I, O, Mat]) extends AnyVal {

    /**
     * Measures time from receiving the first element and completion events - one for each subscriber of this `Flow`.
     */
    def timed[Out, Mat2](measuredOps: Flow[I, O, Mat] ⇒ Flow[I, Out, Mat2],
                         onComplete: FiniteDuration ⇒ Unit): Flow[I, Out, Mat2] =
      Timed.timed[I, O, Out, Mat, Mat2](flow, measuredOps, onComplete)

    /**
     * Measures rolling interval between immediately subsequent `matching(o: O)` elements.
     */
    def timedIntervalBetween(matching: O ⇒ Boolean, onInterval: FiniteDuration ⇒ Unit): Flow[I, O, Mat] =
      Timed.timedIntervalBetween[I, O, Mat](flow, matching, onInterval)
  }

  /**
   * Provides latency measurement utilities on Stream elements.
   *
   * See [[LatencyTimer]]
   */
  implicit class MeteredFlowDsl[I, O, Mat](val flow: Flow[I, O, Mat]) extends AnyVal {

    /**
     * Wraps a given flow and measures the time between input and output. The second parameter is the function which is called for each measured element.
     * The [[TimedResult]] contains the result of the wrapped flow as well in case some logic has to be done.
     *
     * Important Note: the wrapped flow must preserve the order, otherwise timing will be wrong.
     *
     * @param resultFunction side-effect function which gets called with the result
     * @return Flow of the the same shape as the wrapped flow
     */
    def measureLatency(resultFunction: TimedResult[O] ⇒ Unit): Flow[I, O, Mat] =
      LatencyTimer(flow, resultFunction)

    /**
     * Wraps a given flow and measures the time between input and output. The measured result is pushed to a dedicated sink.
     * The [[TimedResult]] contains the result of the wrapped flow as well in case some logic has to be done.
     *
     * Important Note: the wrapped flow must preserve the order, otherwise timing will be wrong.
     *
     * @param sink a sink which will handle the [[TimedResult]]
     * @return Flow of the the same shape as the wrapped flow
     */
    def measureLatency(sink: Graph[SinkShape[TimedResult[O]], Future[Done]]): Flow[I, O, Mat] =
      LatencyTimer(flow, sink)
  }

}
