/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.contrib

import akka.stream._
import akka.stream.scaladsl._
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler, StageLogging}
import akka.util.{ByteString, ByteStringBuilder}
import java.io.{InputStream, OutputStream}
import scala.util.control.NonFatal

/**
 * Streamed zip-file creation Akka streams implementation.
 * Typical usage, assuming an implicit ActorSystem:
 *
 *     import akka.stream._
 *     import akka.stream.scaladsl._
 *
 *     val filesToZip: Iterable[ZipToStreamFlow.ZipSource] = ...
 *     Source(filesToZip)
 *       .via(ZipToStreamFlow(8192))
 *       .runWith(someSink)(ActorMaterializer())
 *
 * The ActorMaterializer can also be implicit.
 */
final class ZipToStreamFlow(bufferSize: Int) extends GraphStage[FlowShape[ZipToStreamFlow.ZipSource, ByteString]] {

  import ZipToStreamFlow._

  val in: Inlet[ZipSource] = Inlet("ZipToStreamFlow.in")
  val out: Outlet[ByteString] = Outlet("ZipToStreamFlow.out")
  override val shape = FlowShape.of(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with StageLogging {
      private val buffer = new ZipBuffer(bufferSize)
      private var currentStream: Option[InputStream] = None
      private var emptyStream = true

      setHandler(
        out,
        new OutHandler {
          override def onPull(): Unit =
            if (isClosed(in)) {
              if (buffer.isEmpty) {
                completeStage()
              } else {
                buffer.close
                push(out, buffer.toByteString)
              }
            } else pull(in)

          override def onDownstreamFinish(): Unit = {
            closeInput()
            buffer.close
            super.onDownstreamFinish()
          }
        }
      )

      setHandler(
        in,
        new InHandler {
          override def onPush(): Unit = {
            val zipSource = grab(in)
            emptyStream = false
            buffer.startEntry(zipSource.filePath)
            val stream = zipSource.streamGenerator()
            currentStream = Some(stream)
            emitMultiple(out, fileChunks(stream, buffer), () => {
              buffer.endEntry()
              closeInput()
            })
          }

          override def onUpstreamFinish(): Unit =
            if (!buffer.isEmpty) {
              buffer.close()
              if (isAvailable(out)) {
                push(out, buffer.toByteString)
              }
            } else if (emptyStream) super.onUpstreamFinish()
        }
      )

      private def closeInput(): Unit = {
        currentStream.foreach(_.close)
        currentStream = None
      }

      private def fileChunks(stream: InputStream, buffer: ZipBuffer): Iterator[ByteString] = {
        // This seems like a good trade-off between single-byte
        // read I/O performance and doubling the ZipBuffer size.
        //
        // And it's still a decent defense against DDOS resource
        // limit attacks.
        val readBuffer = new Array[Byte](1024)
        var done = false

        def result: Stream[ByteString] =
          if (done) Stream.empty
          else {
            try {
              while (!done && buffer.remaining > 0) {
                val bytesToRead = Math.min(readBuffer.length, buffer.remaining)
                val count = stream.read(readBuffer, 0, bytesToRead)
                if (count == -1) {
                  stream.close
                  done = true
                } else buffer.write(readBuffer, count)
              }
              buffer.toByteString #:: result
            } catch {
              case NonFatal(e) =>
                closeInput()
                throw e
            }
          }

        result.iterator.filter(_.nonEmpty)
      }
    }
}

object ZipToStreamFlow {
  final case class ZipSource(filePath: String, streamGenerator: () => InputStream)

  def apply(bufferSize: Int = 64 * 1024) = new ZipToStreamFlow(bufferSize)

  private[this] trait SetZipStream {
    def setOut(out: OutputStream): Unit
  }

  private[ZipToStreamFlow] final class ZipBuffer(val bufferSize: Int) {
    import java.util.zip.{ZipEntry, ZipOutputStream}

    private var builder = new ByteStringBuilder()
    private val zip = new ZipOutputStream(builder.asOutputStream) with SetZipStream {
      // this MUST ONLY be used after flush()!
      override def setOut(newStream: OutputStream): Unit = out = newStream
    }
    private var inEntry = false
    private var closed = false

    def close(): Unit = {
      endEntry()
      closed = true
      zip.close()
    }

    def remaining(): Int = bufferSize - builder.length

    def isEmpty(): Boolean = builder.isEmpty
    def nonEmpty(): Boolean = !isEmpty

    def startEntry(path: String): Unit =
      if (!closed) {
        endEntry()
        zip.putNextEntry(new ZipEntry(path))
        inEntry = true
      }

    def endEntry(): Unit =
      if (!closed && inEntry) {
        inEntry = false
        zip.closeEntry()
      }

    def write(byte: Int): Unit =
      if (!closed && inEntry) zip.write(byte)

    def write(bytes: Array[Byte], length: Int): Unit =
      if (!closed && inEntry) zip.write(bytes, 0, length)

    def toByteString(): ByteString = {
      zip.flush()
      val result = builder.result
      builder = new ByteStringBuilder()
      // set the underlying output for the zip stream to be the buffer
      // directly, so we don't have to copy the zip'd byte array.
      zip.setOut(builder.asOutputStream)
      result
    }
  }
}
