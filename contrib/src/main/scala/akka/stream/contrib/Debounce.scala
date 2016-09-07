package akka.stream.contrib

import akka.stream._
import akka.stream.stage._

import scala.collection.mutable
import scala.concurrent.duration._

private sealed trait QueueResult[+T]
private case class QueueElement[T](element: T) extends QueueResult[T]
private case class NotReadyFor(duration: FiniteDuration) extends QueueResult[Nothing]
private case object QueueEmpty extends QueueResult[Nothing]

private class DebouncingQueue[T, U](keyFunction: (T => U)){
  var values: mutable.Map[U, T] = mutable.Map.empty
  var keyQueue: mutable.ListBuffer[U] = mutable.ListBuffer.empty
  var lastTouchedTimes: mutable.Map[U, Long] = mutable.HashMap.empty

  def insert(e: T): Unit = {
    val key = keyFunction(e)
    keyQueue -= key
    keyQueue.append(key)
    values.put(key, e)
    lastTouchedTimes.put(key, System.nanoTime())
  }

  private def headTime: Option[Long] = {
    keyQueue.headOption.flatMap(lastTouchedTimes.get)
  }

  def headAge: Option[FiniteDuration] = {
    headTime.map(t => (System.nanoTime() - t).nanos)
  }

  def pop(): T = {
    val key = keyQueue.remove(0)
    lastTouchedTimes.remove(key)
    values.remove(key).get
  }

  def popOlderThan(duration: FiniteDuration): QueueResult[T] = {
    headAge match {
      case None => QueueEmpty
      case Some(x) if duration <= x => QueueElement(pop())
      case Some(x) if duration > x => NotReadyFor(x - duration)
    }
  }

  def nonEmpty: Boolean = size > 0

  def isEmpty: Boolean = size == 0

  def size: Int = keyQueue.size

}

/**
  * This stage debounces inputs according to a key function.
  * When an element is received, the identity is calculated,
  * any previous elements with the same key are replaced,
  * and the timer is reset. When elements are pulled, the newest
  * value pertaining to the last-touched key is pushed, given that
  * they are older than the specified duration
  *
  * @param d How long to wait before sending an element
  * @param keyFunction The function to transform elements to a key that
  *                    will be used for debouncing
  * @param bufferMaxSize The maximum number of keys to store at a given
  *                      time. Backpressure will occur if the queue is full
  * @tparam T The type of elements in the stream
  * @tparam U The type of the keys
  */
final class Debounce[T, U](d: FiniteDuration, keyFunction: (T => U), bufferMaxSize: Int = 16) extends GraphStage[FlowShape[T, T]] {
  private[this] def timerName = "DebounceTimer"

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new TimerGraphStageLogic(shape) with InHandler with OutHandler {
    val queue = new DebouncingQueue(keyFunction)
    var willStop = false

    override def onPush(): Unit = {
      val element = grab(in)
      queue.insert(element)
      pushPullIfReady()
    }

    def pullIfPossible(): Unit = {
      if(queue.size < bufferMaxSize && !hasBeenPulled(in)) pull(in)
    }

    def pushPullIfReady(): Unit = {
      if(isAvailable(out)){
        queue.popOlderThan(d) match {
          case QueueElement(e) => push(out, e)
          case NotReadyFor(t) => if(!isTimerActive(timerName)) scheduleOnce(timerName, t)
          case QueueEmpty => Unit
        }
      }
      if (!willStop) pullIfPossible()
      if (willStop && queue.isEmpty) completeStage()
    }

    override def onPull(): Unit = {
      pushPullIfReady()
    }

    override def onUpstreamFinish(): Unit = {
      if (queue.nonEmpty) willStop = true
      else completeStage()
    }

    override def onTimer(timerKey: Any): Unit = {
      pushPullIfReady()
    }

    override def preStart(): Unit = {
      pull(in)
    }

    setHandlers(in, out, this)
  }

  val in = Inlet[T]("in")
  val out = Outlet[T]("out")

  override val shape: FlowShape[T, T] = FlowShape(in, out)
}

object Debounce {
  def apply[T](duration: FiniteDuration): Debounce[T, T] = new Debounce(duration, identity)

  def apply[T, U](duration: FiniteDuration, keyFunction: (T => U)) = new Debounce(duration, keyFunction)
}