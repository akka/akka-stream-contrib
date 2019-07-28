/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.contrib

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.util.zip.ZipInputStream

import akka.NotUsed
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.Future

class MergeToZipFileSpec extends BaseStreamSpec with ScalaFutures {

  val zipFlow = MergeToZipFile.flow()

  "MergeToZipFileFlow" should {

    "merge file to zip file" in {
      val inputFiles = generateInputFiles(5, 100)
      val inputStream = filesToStream(inputFiles)

      val akkaZipped: Future[ByteString] =
        inputStream
          .via(zipFlow)
          .runWith(Sink.fold(ByteString.empty)(_ ++ _))

      unzip(akkaZipped.futureValue) shouldBe inputFiles
    }

    "handle empty stream" in {
      val sources = Source.empty

      val akkaZipped: Future[ByteString] =
        sources
          .via(zipFlow)
          .runWith(Sink.fold(ByteString.empty)(_ ++ _))

      akkaZipped.futureValue shouldBe ByteString.empty
    }
  }

  private def generateInputFiles(numberOfFiles: Int, lengthOfFile: Int): Map[String, Seq[Byte]] = {
    val r = new scala.util.Random(31)
    (1 to numberOfFiles).map(number => s"file-$number" -> r.nextString(lengthOfFile).getBytes.toSeq).toMap
  }

  private def filesToStream(files: Map[String, Seq[Byte]]): Source[(String, Source[ByteString, NotUsed]), NotUsed] = {
    val sourceFiles = files.toList.map {
      case (title, content) =>
        (title, Source(content.grouped(10).map(group => ByteString(group.toArray)).toList))
    }
    Source(sourceFiles)
  }

  private def unzip(bytes: ByteString): Map[String, Seq[Byte]] = {
    var result: Map[String, Seq[Byte]] = Map.empty

    val zis = new ZipInputStream(new ByteArrayInputStream(bytes.toArray))
    val buffer = new Array[Byte](1024)

    try {
      var zipEntry = zis.getNextEntry
      while (zipEntry != null) {
        val baos = new ByteArrayOutputStream()
        val name = zipEntry.getName

        var len = zis.read(buffer)

        while (len > 0) {
          baos.write(buffer, 0, len)
          len = zis.read(buffer)
        }
        result += (name -> baos.toByteArray.toSeq)
        baos.close()
        zipEntry = zis.getNextEntry
      }
    } finally {
      zis.closeEntry()
      zis.close()
    }
    result
  }
}
