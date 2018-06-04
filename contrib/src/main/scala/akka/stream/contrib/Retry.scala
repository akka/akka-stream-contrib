/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.contrib

import akka.stream._
import akka.stream.scaladsl._
import akka.stream.stage._
import com.typesafe.config.ConfigFactory

import scala.collection.immutable
import scala.concurrent.duration.Duration
import scala.util.{ Success, Try }

/**
 * This object defines methods for retry operations.
 */
object Retry {

  /**
   * Retry flow factory. given a flow that produces `Try`s, this wrapping flow may be used to try
   * and pass failed elements through the flow again. More accurately, the given flow consumes a tuple
   * of `input` & `state`, and produces a tuple of `Try` of `output` and `state`.
   * If the flow emits a failed element (i.e. `Try` is a `Failure`), the `retryWith` function is fed with the
   * `state` of the failed element, and may produce a new input-state tuple to pass through the original flow.
   * The function may also yield `None` instead of `Some((input,state))`, which means not to retry a failed element.
   *
   * IMPORTANT CAVEAT:
   * The given flow must not change the number of elements passing through it (i.e. it should output
   * exactly one element for every received element). Ignoring this, will have an unpredicted result,
   * and may result in a deadlock.
   *
   * @param flow the flow to retry
   * @param retryWith if output was failure, we can optionaly recover from it,
   *        and retry with a new pair of input & new state we get from this function.
   * @tparam I input elements type
   * @tparam O output elements type
   * @tparam S state to create a new `(I,S)` to retry with
   * @tparam M materialized value type
   */
  def apply[I, O, S, M](flow: Graph[FlowShape[(I, S), (Try[O], S)], M])(retryWith: S => Option[(I, S)]): Graph[FlowShape[(I, S), (Try[O], S)], M] = {
    GraphDSL.create(flow) { implicit b => origFlow =>
      import GraphDSL.Implicits._

      val retry = b.add(new RetryCoordinator[I, S, O](retryWith))

      retry.out2 ~> origFlow ~> retry.in2

      FlowShape(retry.in1, retry.out1)
    }
  }

  /**
   * Factory for multiple retries flow. similar to the simple retry, but this will allow to
   * break down a "heavy" element which failed into multiple "thin" elements, that may succeed individually.
   * Since it's easy to inflate elements in retry cycle, there's also a limit parameter,
   * that will limit the amount of generated elements by the `retryWith` function,
   * and will fail the stage if that limit is exceeded.
   * Passing `Some(Nil)` is valid, and will result in filtering out the failure quietly, without
   * emitting a failed `Try` element.
   *
   * In case of `Failure` element in the flow, the elements created from `retryWith` function are handled
   * before pulling new elements from upstream.
   *
   * IMPORTANT CAVEAT:
   * The given flow must not change the number of elements passing through it (i.e. it should output
   * exactly one element for every received element). Ignoring this, will have an unpredicted result,
   * and may result in a deadlock.
   *
   * @param retriesLimit since every retry can generate more elements,
   *        the inner queue can get too big. if the limit is reached,
   *        the stage will fail.
   * @param bufferLimit max number of concurrent elements that can be processed at a time.
   * @param flow the flow to retry
   * @param retryWith if output was failure, we can optionaly recover from it,
   *        and retry with a sequence of input & new state pairs we get from this function.
   * @tparam I input elements type
   * @tparam O output elements type
   * @tparam S state to create a new `(I,S)` to retry with
   * @tparam M materialized value type
   */
  def concat[I, O, S, M](retriesLimit: Long, bufferLimit: Long, flow: Graph[FlowShape[(I, S), (Try[O], S)], M])(retryWith: S => Option[immutable.Iterable[(I, S)]]): Graph[FlowShape[(I, S), (Try[O], S)], M] = {
    GraphDSL.create(flow) { implicit b => origFlow =>
      import GraphDSL.Implicits._

      val retry = b.add(new RetryConcatCoordinator[I, S, O](retriesLimit, bufferLimit, retryWith))

      retry.out2 ~> origFlow ~> retry.in2

      FlowShape(retry.in1, retry.out1)
    }
  }

  private[akka] class RetryCoordinator[I, S, O](retryWith: S => Option[(I, S)]) extends GraphStage[BidiShape[(I, S), (Try[O], S), (Try[O], S), (I, S)]] {
    val in1 = Inlet[(I, S)]("Retry.ext.in")
    val out1 = Outlet[(Try[O], S)]("Retry.ext.out")
    val in2 = Inlet[(Try[O], S)]("Retry.int.in")
    val out2 = Outlet[(I, S)]("Retry.int.out")
    override val shape = BidiShape[(I, S), (Try[O], S), (Try[O], S), (I, S)](in1, out1, in2, out2)
    override def createLogic(attributes: Attributes) = new GraphStageLogic(shape) {
      var elementInCycle = false
      var pending: (I, S) = null

      setHandler(in1, new InHandler {
        override def onPush() = {
          val is = grab(in1)
          if (!hasBeenPulled(in2)) pull(in2)
          push(out2, is)
          elementInCycle = true
        }

        override def onUpstreamFinish() = {
          if (!elementInCycle)
            completeStage()
        }
      })

      setHandler(out1, new OutHandler {
        override def onPull() = {
          if (isAvailable(out2)) pull(in1)
          else pull(in2)
        }
      })

      setHandler(in2, new InHandler {
        override def onPush() = {
          elementInCycle = false
          grab(in2) match {
            case s @ (_: Success[O], _) => pushAndCompleteIfLast(s)
            case failure @ (_, s) => retryWith(s).fold(pushAndCompleteIfLast(failure)) { is =>
              pull(in2)
              if (isAvailable(out2)) {
                push(out2, is)
                elementInCycle = true
              } else pending = is
            }
          }
        }
      })

      def pushAndCompleteIfLast(elem: (Try[O], S)): Unit = {
        push(out1, elem)
        if (isClosed(in1))
          completeStage()
      }

      setHandler(out2, new OutHandler {
        override def onPull() = {
          if (isAvailable(out1) && !elementInCycle) {
            if (pending ne null) {
              push(out2, pending)
              pending = null
              elementInCycle = true
            } else if (!hasBeenPulled(in1)) {
              pull(in1)
            }
          }
        }

        override def onDownstreamFinish() = {
          //Do Nothing, intercept completion as downstream
        }
      })
    }
  }

  private[akka] class RetryConcatCoordinator[I, S, O](retriesLimit: Long, bufferLimit: Long, retryWith: S => Option[immutable.Iterable[(I, S)]]) extends GraphStage[BidiShape[(I, S), (Try[O], S), (Try[O], S), (I, S)]] {
    val in1 = Inlet[(I, S)]("RetryConcat.ext.in")
    val out1 = Outlet[(Try[O], S)]("RetryConcat.ext.out")
    val in2 = Inlet[(Try[O], S)]("RetryConcat.int.in")
    val out2 = Outlet[(I, S)]("RetryConcat.int.out")
    override val shape = BidiShape[(I, S), (Try[O], S), (Try[O], S), (I, S)](in1, out1, in2, out2)

    override def createLogic(attributes: Attributes) = new GraphStageLogic(shape) {
      var numElementsInCycle = 0
      val queueRetries = scala.collection.mutable.Queue.empty[(I, S)]
      val queueOut1 = scala.collection.mutable.Queue.empty[(Try[O], S)]
      lazy val timeout = Duration.fromNanos(ConfigFactory.load().getDuration("akka.stream.contrib.retry-timeout").toNanos)

      setHandler(in1, new InHandler {
        override def onPush() = {
          val is = grab(in1)
          if (isAvailable(out2)) {
            push(out2, is)
            numElementsInCycle += 1
          } else queueRetries.enqueue(is)
        }

        override def onUpstreamFinish() = {
          if (numElementsInCycle == 0 && queueRetries.isEmpty) {
            if (queueOut1.isEmpty) completeStage()
            else emitMultiple(out1, queueOut1.iterator, () => completeStage())
          }
        }
      })

      setHandler(out1, new OutHandler {
        override def onPull() = {
          // prioritize pushing queued element if available
          if (queueOut1.isEmpty) pull(in2)
          else push(out1, queueOut1.dequeue())
        }
      })

      setHandler(in2, new InHandler {
        override def onPush() = {
          numElementsInCycle -= 1
          grab(in2) match {
            case s @ (_: Success[O], _) => pushAndCompleteIfLast(s)
            case failure @ (_, s) => retryWith(s).fold(pushAndCompleteIfLast(failure)) { xs =>
              if (xs.size + queueRetries.size > retriesLimit) failStage(new IllegalStateException(s"Queue limit of $retriesLimit has been exceeded. Trying to append ${xs.size} elements to a queue that has ${queueRetries.size} elements."))
              else {
                xs.foreach(queueRetries.enqueue(_))
                if (queueRetries.isEmpty) {
                  if (isClosed(in1) && queueOut1.isEmpty) completeStage()
                  else pull(in2)
                } else {
                  pull(in2)
                  if (isAvailable(out2)) {
                    val elem = queueRetries.dequeue()
                    push(out2, elem)
                    numElementsInCycle += 1
                  }
                }
              }
            }
          }
        }
      })

      def pushAndCompleteIfLast(elem: (Try[O], S)): Unit = {
        if (isAvailable(out1) && queueOut1.isEmpty) {
          push(out1, elem)
        } else if (queueOut1.size + 1 > bufferLimit) {
          failStage(new IllegalStateException(s"Buffer limit of $bufferLimit has been exceeded. Trying to append 1 element to a buffer that has ${queueOut1.size} elements."))
        } else {
          queueOut1.enqueue(elem)
        }

        if (isClosed(in1) && queueRetries.isEmpty && numElementsInCycle == 0 && queueOut1.isEmpty) {
          completeStage()
        }
      }

      setHandler(out2, new OutHandler {
        override def onPull() = {
          if (queueRetries.isEmpty) {
            if (!hasBeenPulled(in1) && !isClosed(in1)) {
              pull(in1)
            }
          } else {
            push(out2, queueRetries.dequeue())
            numElementsInCycle += 1
          }
        }

        override def onDownstreamFinish() = {
          materializer.scheduleOnce(timeout, new Runnable {
            override def run() = {
              getAsyncCallback[Unit] { _ =>
                if (!isClosed(in2)) {
                  failStage(new IllegalStateException(s"inner flow canceled only upstream, while downstream remain available for $timeout"))
                }
              }.invoke(())
            }
          })
        }
      })
    }
  }

}
