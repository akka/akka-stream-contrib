/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.contrib

import java.util.concurrent.atomic.AtomicBoolean
import akka.actor.Cancellable
import akka.stream.Attributes
import akka.stream.impl.Unfold
import akka.stream.scaladsl.Source

object SourceRepeatEval {
  def apply[T](element: => T): Source[T, Cancellable] = {
    val c: Cancellable = new Cancellable {
      private val stopped: AtomicBoolean = new AtomicBoolean(false)
      override def cancel(): Boolean = stopped.compareAndSet(false, true)
      override def isCancelled: Boolean = stopped.get()
    }

    def nextElement(): T = element

    def nextStep: Unit => Option[(Unit, T)] = {
      _ =>
        {
          if (c.isCancelled) {
            None
          } else {
            Some(() -> nextElement())
          }
        }
    }

    Source
      .fromGraph(new Unfold[Unit, T]((), nextStep))
      .withAttributes(Attributes.name("repeat-eval"))
      .mapMaterializedValue(_ => c)
  }
}
