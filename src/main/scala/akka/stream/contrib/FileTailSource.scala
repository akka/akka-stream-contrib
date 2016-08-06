/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.contrib

import java.nio.ByteBuffer
import java.nio.channels.{ AsynchronousFileChannel, CompletionHandler }
import java.nio.file.{ Files, Path, StandardOpenOption }

import akka.NotUsed
import akka.stream.stage._
import akka.stream.{ Attributes, Outlet, SourceShape }
import akka.util.ByteString

import scala.concurrent.duration.FiniteDuration
import scala.util.{ Failure, Success, Try }

object FileTailSource {

  /**
   * INTERNAL API
   *
   * Single reusable completion handler instance
   */
  private object TailCompletionHandler extends CompletionHandler[Integer, AsyncCallback[Try[Integer]]] {
    override def completed(result: Integer, attachment: AsyncCallback[Try[Integer]]): Unit =
      attachment.invoke(Success(result))
    override def failed(ex: Throwable, attachment: AsyncCallback[Try[Integer]]): Unit =
      attachment.invoke(Failure(ex))
  }

}

/**
 * Read the entire contents of a file, and then when the end is reached, keep reading
 * newly appended data. Like the unix command `tail -f`.
 *
 * Aborting the stage can be done by combining with a [[akka.stream.KillSwitch]]
 *
 * @param path a file path to tail
 * @param maxChunkSize The max emitted size of the `ByteString`s
 * @param startingPosition Offset into the file to start reading
 * @param pollingInterval When the end has been reached, look for new content with this interval
 */
final class FileTailSource(path: Path, maxChunkSize: Int, startingPosition: Long, pollingInterval: FiniteDuration) extends GraphStage[SourceShape[ByteString]] {
  import FileTailSource.TailCompletionHandler

  val out = Outlet[ByteString]("FileTailSource.out")
  override val shape: SourceShape[ByteString] = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new TimerGraphStageLogic(shape) with OutHandler {
      assert(Files.isReadable(path), s"path $path is not readable")
      implicit def ec = materializer.executionContext

      val buffer = ByteBuffer.allocate(maxChunkSize)
      val channel = AsynchronousFileChannel.open(path, StandardOpenOption.READ)
      var position = startingPosition
      var chunkCallback: AsyncCallback[Try[Integer]] = _

      override def preStart(): Unit = {
        chunkCallback = getAsyncCallback[Try[Integer]] {
          case Success(readBytes) =>
            if (readBytes > 0) {
              buffer.flip()
              push(out, ByteString.fromByteBuffer(buffer))
              position += readBytes
              buffer.clear()
            } else {
              // hit end, try again in a while
              scheduleOnce(NotUsed, pollingInterval)
            }

          case Failure(ex) =>
            failStage(ex)
        }
      }

      override protected def onTimer(timerKey: Any): Unit = {
        onPull()
      }

      override def onPull(): Unit = {
        channel.read(buffer, position, chunkCallback, TailCompletionHandler)
      }

      override def postStop(): Unit = {
        if (channel.isOpen) channel.close()
      }

      setHandler(out, this)
    }
}