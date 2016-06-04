/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.contrib

import akka.stream.stage.{ GraphStage, GraphStageLogic, InHandler, OutHandler }
import akka.stream.{ Attributes, FlowShape, Inlet, Outlet }

object AlphabeticFraming {

  final case class Frame(value: String, isAlphabetic: Boolean)

  private final val alphabet = 'A'.to('Z').to[Set]

  def apply(): AlphabeticFraming = new AlphabeticFraming

  private def isAlphabetic(c: Char) = alphabet.contains(c.toUpper)
}

class AlphabeticFraming private extends GraphStage[FlowShape[String, AlphabeticFraming.Frame]] {
  import AlphabeticFraming._

  override val shape = FlowShape(Inlet[String]("alphabeticFraming.in"), Outlet[Frame]("alphabeticFraming.out"))

  override def createLogic(attributes: Attributes) = new GraphStageLogic(shape) {
    import shape._

    private var frame = Option.empty[Frame]

    setHandler(in, new InHandler {
      override def onPush() = {
        val s = grab(in)
        val frames = s.foldLeft(frame.toVector) { (frames, c) =>
          frames match { // format: OFF
            case _ :+ last if last.isAlphabetic ^ isAlphabetic(c) => frames :+ Frame(c.toString, isAlphabetic(c))
            case init :+ last                                     => init :+ last.copy(last.value + c)
            case _                                                => Vector(Frame(c.toString, isAlphabetic(c)))
          } // format: ON
        }
        emitMultiple(out, frames.init)
        frame = Some(frames.last)
      }

      override def onUpstreamFinish() = {
        emitMultiple(out, frame.iterator)
        super.onUpstreamFinish()
      }
    })

    setHandler(out, new OutHandler {
      override def onPull() = pull(in)
    })
  }
}
