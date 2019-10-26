/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.contrib

import akka.stream.scaladsl.Source
import akka.util.ByteString
import org.scalatest.concurrent.ScalaFutures

class TarballFlowSpec extends BaseStreamSpec with ScalaFutures {
  "A TarballFlow" should {
    "convert an empty source of files into an empty tarball source" in {
      val source = Source.empty
      val tarball = source.via(TarballFlow.generate).runFold(ByteString.empty)(_ ++ _).futureValue
      tarball should have size (0)
    }

    "convert a nested source of files into a flat tarball source" in {
      val file1 = ("file1.txt", ByteString("file1"))
      val file2 = (
        "path-".padTo(40, 'x') + "/" + "path-".padTo(40, 'x') + "/" + "file2-".padTo(80, 'x') + ".txt",
        ByteString("file12")
      )
      val source = Source(List(file1, file2).map {
        case (name, bytes) =>
          (name, bytes.size.toLong, Source.single(bytes))
      })
      val tarball = source.via(TarballFlow.generate).runFold(ByteString.empty)(_ ++ _).futureValue
      tarball should have size (2048)
    }
  }
}
