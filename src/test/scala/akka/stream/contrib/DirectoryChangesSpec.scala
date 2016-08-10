/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.contrib

import java.io.{ File, IOException }
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes
import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import akka.stream.testkit.TestSubscriber
import akka.testkit.TestKit
import com.google.common.jimfs.{ Configuration, Jimfs, WatchServiceConfiguration }
import com.typesafe.config.ConfigFactory
import org.scalatest.{ BeforeAndAfterAll, Matchers, WordSpec }
import scala.concurrent.duration._

class DirectoryChangesSpec extends WordSpec with Matchers with BeforeAndAfterAll {

  private implicit val system = ActorSystem("DirectoryChangeSpec")
  private implicit val mat = ActorMaterializer()

  val fs = Jimfs.newFileSystem(
    "directory-change-spec",
    Configuration.forCurrentPlatform()
      .toBuilder
      .setWatchServiceConfiguration(WatchServiceConfiguration.polling(10, TimeUnit.MILLISECONDS))
      .build()
  )
  val testDir = fs.getPath(s"testdir")

  override protected def beforeAll(): Unit = {
    Files.createDirectory(testDir)
  }

  "DirectoryChanges" should {

    "emit on directory changes" in {
      val probe = TestSubscriber.probe[(Path, DirectoryChanges.Change)]()
      DirectoryChanges.apply(testDir, 250.millis, 200).runWith(Sink.fromSubscriber(probe))

      probe.request(1)
      // race here, since we don't know if the request reaches the stage before
      // we create the file
      Thread.sleep(250)

      val createdFile = Files.createFile(testDir.resolve("test1file1.sample"))

      val (path1, change1) = probe.expectNext()
      change1 shouldEqual DirectoryChanges.Change.Creation
      path1 shouldEqual createdFile

      Files.write(path1, "Some data".getBytes())

      probe.request(1)
      val (path2, change2) = probe.expectNext()
      change2 shouldEqual DirectoryChanges.Change.Modification
      path2 shouldEqual createdFile

      Files.delete(path2)

      probe.request(1)
      val (path3, change3) = probe.expectNext()
      change3 shouldEqual DirectoryChanges.Change.Deletion
      path3 shouldEqual createdFile

      probe.cancel()
    }

    "emit on a bunch of changes" in {
      val probe = TestSubscriber.probe[(Path, DirectoryChanges.Change)]()
      val numberOfChanges = 50
      DirectoryChanges.apply(testDir, 250.millis, 50 * 2).runWith(Sink.fromSubscriber(probe))

      probe.request(numberOfChanges.toLong)
      // race here, since we don't know if the request reaches the stage before
      // we create the file
      Thread.sleep(100)

      val halfRequested = numberOfChanges / 2
      val paths = for (n <- 0 until halfRequested) yield Files.createFile(testDir.resolve(s"test2files$n"))

      (0 until halfRequested).foreach { n =>
        probe.expectNext()
      }

      paths.foreach(Files.delete)

      (0 until halfRequested).foreach { n =>
        probe.expectNext()
      }

      probe.cancel()
    }

  }

  override protected def afterAll(): Unit = {
    fs.close()
    TestKit.shutdownActorSystem(system)
  }
}
