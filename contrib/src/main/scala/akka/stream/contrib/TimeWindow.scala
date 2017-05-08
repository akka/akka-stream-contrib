/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.contrib

import akka.NotUsed
import akka.stream.scaladsl.Flow

import scala.concurrent.duration.FiniteDuration

object TimeWindow {
  /**
   * Aggregates data for predefined amount of time. The computed aggregate is emitted after window expires, thereafter a new window starts.
   *
   * For example:
   * {{{
   * Source.tick(0.seconds, 100.milliseconds, 1)
   *    .via(TimeWindow(500.milliseconds, eager = false)(identity[Int])(_ + _))
   * }}}
   * will emit sum of 1s after each 500ms - so you could expect a stream of 5s if timers were ideal
   * If ''eager'' had been true in the following example, you would have observed initial 1 with no delay, followed by stream of sums
   *
   * @param of window duration
   * @param eager if ''true'' emits the very first seed with no delay, otherwise the first element emitted is the result of the first time-window aggregation
   * @param seed provides the initial state when a new window starts
   * @param aggregate produces updated aggregate
   * @tparam A type of incoming element
   * @tparam S type of outgoing (aggregated) element
   * @return flow implementing time-window aggregation
   */
  def apply[A, S](of: FiniteDuration, eager: Boolean = true)(seed: A => S)(aggregate: (S, A) ⇒ S): Flow[A, S, NotUsed] =
    Flow[A].conflateWithSeed(seed)(aggregate).via(new Pulse(of, eager))
}
