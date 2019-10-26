/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.contrib

import java.lang.Long.toOctalString
import java.time.Instant

import akka.NotUsed
import akka.stream.scaladsl.{Flow, Source}
import akka.util.ByteString

final case class TarballHeader(fullFileName: String, fileSize: Long, lastModification: Instant = Instant.now) {
  private val fileNamePrefix = Option(fullFileName.split("/").init).filter(_.nonEmpty).map(_.mkString("/"))
  private val fileName = fullFileName.split("/").last

  require(
    fileNamePrefix.forall(fnp => fnp.length > 0 && fnp.length <= 154),
    "File name prefix must be between 1 and 154 characters long"
  )
  require(fileName.length > 0 && fileName.length <= 99, "File name must be between 1 and 99 characters long")

  def bytes: ByteString = {
    val withoutChecksum = bytesWithoutChecksum
    val checksumLong = withoutChecksum.foldLeft(0L)((sum, byte) => sum + byte)
    val checksumBytes = ByteString(toOctalString(checksumLong).reverse.padTo(6, '0').take(6).reverse) ++ ByteString(
      new Array[Byte](1) ++ ByteString(" ")
    )
    val withChecksum = withoutChecksum.take(148) ++ checksumBytes ++ withoutChecksum.drop(148 + 8)
    withChecksum.compact
  }

  private def bytesWithoutChecksum: ByteString = {
    // [0, 100)
    val fileNameBytes = withPadding(ByteString(fileName), 100)
    // [100, 108)
    val fileModeBytes = withPadding(ByteString("0755"), 8)
    // [108, 116)
    val ownerIdBytes = withPadding(ByteString.empty, 8)
    // [116, 124)
    val groupIdBytes = withPadding(ByteString.empty, 8)
    // [124, 136)
    val fileSizeBytes = withPadding(ByteString("0" + toOctalString(fileSize)), 12)
    // [136, 148)
    val lastModificationBytes = withPadding(ByteString(toOctalString(lastModification.getEpochSecond)), 12)
    // [148, 156)
    val checksumPlaceholderBytes = ByteString("        ")
    // [156, 157)
    val linkIndicatorBytes = withPadding(ByteString.empty, 1)
    // [157, 257)
    val linkFileNameBytes = withPadding(ByteString.empty, 100)
    // [257, 263)
    val ustarIndicatorBytes = ByteString("ustar") ++ ByteString(new Array[Byte](1))
    // [263, 265)
    val ustarVersionBytes = ByteString(new Array[Byte](2))
    // [265, 297)
    val ownerNameBytes = withPadding(ByteString.empty, 32)
    // [297, 329)
    val groupNameBytes = withPadding(ByteString.empty, 32)
    // [329, 337)
    val deviceMajorNumberBytes = withPadding(ByteString.empty, 8)
    // [337, 345)
    val deviceMinorNumberBytes = withPadding(ByteString.empty, 8)
    // [345, 500)
    val fileNamePrefixBytes = withPadding(fileNamePrefix.map(ByteString.apply).getOrElse(ByteString.empty), 155)

    withPadding(
      fileNameBytes ++ fileModeBytes ++ ownerIdBytes ++ groupIdBytes ++ fileSizeBytes ++ lastModificationBytes ++ checksumPlaceholderBytes ++ linkIndicatorBytes ++ linkFileNameBytes ++ ustarIndicatorBytes ++ ustarVersionBytes ++ ownerNameBytes ++ groupNameBytes ++ deviceMajorNumberBytes ++ deviceMinorNumberBytes ++ fileNamePrefixBytes,
      512
    )
  }

  private def withPadding(bytes: ByteString, targetSize: Int): ByteString = {
    require(bytes.size <= targetSize)
    if (bytes.size < targetSize) bytes ++ ByteString(new Array[Byte](targetSize - bytes.size))
    else bytes
  }
}

object TarballFlow {
  def generate: Flow[(String, Long, Source[ByteString, _]), ByteString, NotUsed] =
    Flow[(String, Long, Source[ByteString, _])]
      .flatMapConcat {
        case (fileName, fileSize, bytes) =>
          val header = TarballHeader(fileName, fileSize)
          Source.single(header.bytes).concat(bytes).concat(Source.single(padding(fileSize)))
      }

  def generateStrict: Flow[(String, ByteString), ByteString, NotUsed] =
    Flow[(String, ByteString)]
      .map {
        case (fileName, bytes) =>
          (fileName, bytes.size.toLong, Source.single(bytes))
      }
      .via(generate)

  private def padding(fileSize: Long): ByteString =
    ByteString(new Array[Byte](if (fileSize % 512 > 0) (512 - fileSize % 512).toInt else 0))
}
