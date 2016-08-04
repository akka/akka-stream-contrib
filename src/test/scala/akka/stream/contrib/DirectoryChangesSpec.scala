/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.contrib

import java.io.{ File, IOException }
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import akka.stream.testkit.TestSubscriber
import akka.testkit.TestKit
import com.typesafe.config.ConfigFactory
import org.scalatest.{ BeforeAndAfterAll, Matchers, WordSpec }

class DirectoryChangesSpec extends WordSpec with Matchers with BeforeAndAfterAll {

  protected implicit val system = {
    def config = ConfigFactory.parseString(
      s"akka.test.single-expect-default=5s"
    ) // this is one slow test...
      .withFallback(ConfigFactory.load())
    ActorSystem("default", config)
  }
  protected implicit val mat = ActorMaterializer()

  val fs = FileSystems.getDefault
  val tmpDir = fs.getPath(sys.props("java.io.tmpdir"), s"dir-changes-spec-${System.nanoTime()}")

  override protected def beforeAll(): Unit = {
    tmpDir.toFile.mkdir()
  }

  "DirectoryChanges" should {

    "emit on directory changes" in {
      val probe = TestSubscriber.probe[(Path, DirectoryChanges.Change)]()
      DirectoryChanges(tmpDir).runWith(Sink.fromSubscriber(probe))

      probe.request(1)
      // race here, since we don't know if the request reaches the stage before
      // we create the file
      Thread.sleep(100)

      val createdFile = File.createTempFile("file1", ".sample", tmpDir.toFile)

      val (path1, change1) = probe.expectNext()
      change1 shouldEqual DirectoryChanges.Created
      path1.toFile shouldEqual createdFile

      Files.write(path1, "Some data".getBytes())

      probe.request(1)
      val (path2, change2) = probe.expectNext()
      change2 shouldEqual DirectoryChanges.Modified
      path2.toFile shouldEqual createdFile

      Files.delete(path2)

      probe.request(1)
      val (path3, change3) = probe.expectNext()
      change3 shouldEqual DirectoryChanges.Deleted
      path3.toFile shouldEqual createdFile

      probe.cancel()
    }

    "emit on a bunch of changes" in {
      val probe = TestSubscriber.probe[(Path, DirectoryChanges.Change)]()
      val numberOfChanges = 50
      DirectoryChanges(tmpDir, maxBufferSize = 50 * 2).runWith(Sink.fromSubscriber(probe))

      probe.request(numberOfChanges.toLong)
      // race here, since we don't know if the request reaches the stage before
      // we create the file
      Thread.sleep(100)

      val halfRequested = numberOfChanges / 2
      val files = for (n <- 0 until halfRequested) yield File.createTempFile(s"files$n", ".sample", tmpDir.toFile)

      (0 until halfRequested).foreach { n =>
        probe.expectNext()
      }

      files.foreach(f => Files.delete(f.toPath))

      (0 until halfRequested).foreach { n =>
        probe.expectNext()
      }

      probe.cancel()
    }

  }

  override protected def afterAll(): Unit = {
    tmpDir.toFile.listFiles().foreach(_.delete())
    Files.delete(tmpDir)
    TestKit.shutdownActorSystem(system)
  }
}
