/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.contrib

import akka.NotUsed
import akka.stream._
import akka.stream.scaladsl.Flow

import scala.collection.immutable
import scala.concurrent.duration._

object IntervalBasedRateLimiter {

  /**
    * Specialized type of rate limiter which emits batches of elements (with size limited by the @maxBatchSize parameter)
    * with a minimum time interval of @minInterval.
    *
    * Because the next emit is scheduled after we downstream the current batch, the effective throughput,
    * depending on the minimal interval length, may never reach the maximum allowed one.
    * You can minimize these delays by sending bigger batches less often.
    *
    * @param minInterval  minimal pause to be kept before downstream the next batch. Should be >= 10 milliseconds.
    * @param maxBatchSize maximum number of elements to send in the single batch
    * @tparam T type of element
    */
  def create[T](minInterval: FiniteDuration, maxBatchSize: Int): Graph[FlowShape[T, immutable.Seq[T]], NotUsed] =
    Flow[T].groupedWithin(maxBatchSize, minInterval).via(DelayFlow[immutable.Seq[T]](minInterval))

}
