/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.contrib

import java.util.zip.{ZipEntry, ZipOutputStream}
import akka.NotUsed
import akka.stream._
import akka.stream.scaladsl.{Flow, Source}
import akka.stream.stage._
import akka.util.{ByteString, ByteStringBuilder}

/**
 *  Merges many file streams into one stream with ZIP file.
 *
 *  One file is represented as a tuple (String, Source[ByteString, NotUsed]).
 *  This matches Alpakka S3 connector implementation. Can be used for e.g. download many S3 files in one ZIP file.
 *
 *  Connected with source of type Source[(String, Source[ByteString, NotUsed]), NotUsed] will result in Source[ByteString, NotUsed].
 */
object MergeToZipFile {

  private val startFileWord = "$START$"
  private val endFileWord = "$END$"
  private val separator: Char = '|'

  private val zipStage = new FileZipStage()

  def flow(): Flow[(String, Source[ByteString, Any]), ByteString, NotUsed] =
    Flow[(String, Source[ByteString, Any])]
      .flatMapConcat {
        case (path, stream) =>
          val prependElem: Source[ByteString, Any] = Source.single(createStartingByteString(path))
          val appendElem: Source[ByteString, Any] = Source.single(ByteString(endFileWord))
          stream.prepend(prependElem).concat(appendElem).via(zipStage)
      }

  private def createStartingByteString(path: String): ByteString =
    ByteString(s"$startFileWord$separator$path")

  private def getPathFromByteString(b: ByteString): String = {
    val splitted = b.utf8String.split(separator)
    if (splitted.length == 1) {
      ""
    } else if (splitted.length == 2) {
      splitted.tail.head
    } else {
      splitted.tail.mkString(separator.toString)
    }
  }

  class FileZipStage extends GraphStage[FlowShape[ByteString, ByteString]] {
    val in: Inlet[ByteString] = Inlet("FileZipStage.in")
    val out: Outlet[ByteString] = Outlet("FileZipStage.out")
    override val shape: FlowShape[ByteString, ByteString] = FlowShape.of(in, out)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
      private val builder = new ByteStringBuilder()
      private val zip = new ZipOutputStream(builder.asOutputStream)
      private var emptyStream = true

      setHandler(
        out,
        new OutHandler {
          override def onPull(): Unit =
            if (isClosed(in)) {
              val result = builder.result
              if (result.nonEmpty) {
                push(out, result)
              }
              builder.clear()
              completeStage()
            } else {
              pull(in)
            }
        }
      )

      setHandler(
        in,
        new InHandler {
          override def onPush(): Unit = {
            emptyStream = false
            val element = grab(in)
            element match {
              case b: ByteString if b.utf8String.startsWith(MergeToZipFile.startFileWord) =>
                val name = MergeToZipFile.getPathFromByteString(b)
                zip.putNextEntry(new ZipEntry(name))
              case b: ByteString if b.utf8String == MergeToZipFile.endFileWord =>
                zip.closeEntry()
              case b: ByteString =>
                val array = b.toArray
                zip.write(array, 0, array.length)
            }
            zip.flush()
            val result = builder.result
            if (result.nonEmpty) {
              builder.clear()
              push(out, result)
            } else {
              pull(in)
            }
          }

          override def onUpstreamFinish(): Unit =
            if (emptyStream) {
              zip.close()
              super.onUpstreamFinish()
            }
        }
      )
    }
  }
}
