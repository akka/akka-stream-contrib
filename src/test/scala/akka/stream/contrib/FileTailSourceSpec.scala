/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.contrib

import java.nio.file.{ FileSystem, FileSystems, Files }

import akka.stream.scaladsl._
import akka.util.ByteString

import scala.concurrent.duration._
import scala.io.StdIn

class FileTailSourceSpec extends BaseStreamSpec {
  override protected def autoFusing: Boolean = true

  val fs = FileSystems.getDefault

  "The FileTailSource" should {

    /*   TODO figure out how to test

    this is how I confirmed it works so far:
    "tail a file" in {

      val source = Source.fromGraph(new FileTailSource(
        fs.getPath("/var/log/system.log"),
        startingPosition = 0L,
        maxChunkSize = 8192,
        pollingInterval = 1.second
      ))

      source.via(Framing.delimiter(ByteString("\n"), maximumFrameLength = 8192))
        .map(_.utf8String)
        .runForeach(println)

      println("Running, ENTER to stop")
      StdIn.readLine()

    } */

  }

}
