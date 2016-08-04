/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.contrib

import java.io.File
import java.nio.file.StandardWatchEventKinds._
import java.nio.file._

import akka.NotUsed
import akka.stream.{ ActorAttributes, Attributes, Outlet, SourceShape }
import akka.stream.scaladsl.Source
import akka.stream.stage.{ GraphStage, GraphStageLogic, OutHandler, TimerGraphStageLogic }
import akka.japi.Pair
import com.sun.nio.file.SensitivityWatchEventModifier

import scala.collection.immutable.Queue
import scala.collection.JavaConverters._
import scala.concurrent.duration._

object DirectoryChanges {
  sealed trait Change
  case object Modified extends Change
  case object Created extends Change
  case object Deleted extends Change

  /**
   * Scala API
   *
   * Watches a file system directory and streams change events from it.
   *
   * @see [[DirectoryChanges]] for details
   */
  def apply(directory: Path, pollInterval: FiniteDuration = 500.millis, maxBufferSize: Int = 200): Source[(Path, Change), NotUsed] =
    Source.fromGraph(new DirectoryChanges(directory, pollInterval, maxBufferSize))

  /**
   * Java API
   * @see [[DirectoryChanges]] for details
   */
  def forDirectory(directory: Path): Source[Pair[Path, DirectoryChanges.Change], NotUsed] = apply(directory).map(t => Pair.create(t._1, t._2))

  /**
   * Java API
   * @see [[DirectoryChanges]] for details
   */
  def forDirectory(directory: Path, pollInterval: FiniteDuration, maxBufferSize: Int): Source[Pair[Path, DirectoryChanges.Change], NotUsed] =
    apply(directory, pollInterval, maxBufferSize).map(t => Pair.create(t._1, t._2))

  private def japiKindToChange(orig: WatchEvent.Kind[Path]): Change = orig match {
    case StandardWatchEventKinds.ENTRY_CREATE => Created
    case StandardWatchEventKinds.ENTRY_DELETE => Deleted
    case StandardWatchEventKinds.ENTRY_MODIFY => Modified
    case _                                    => throw new IllegalArgumentException(s"unknown event kind $orig")
  }

  private val defaultAttributes = Attributes.name("DirectoryChanges")

}

/**
 * Watches a file system directory and streams change events from it.
 *
 * Note that the JDK watcher is notoriously slow on some platform (up to 1s after event actually happened on OSX for example)
 *
 * @param directoryPath Directory to watch
 * @param pollInterval Interval between polls to the JDK watch service when a push comes in and there was no changes, if
 *                     the JDK implementation is slow, it will not help lowering this
 * @param maxBufferSize Maximum number of buffered directory changes before the stage fails
 */
final class DirectoryChanges(directoryPath: Path, pollInterval: FiniteDuration, maxBufferSize: Int) extends GraphStage[SourceShape[(Path, DirectoryChanges.Change)]] {
  import DirectoryChanges._
  private val out: Outlet[(Path, Change)] = Outlet("DirectoryChanges.out")

  override protected def initialAttributes: Attributes = defaultAttributes

  override def shape: SourceShape[(Path, Change)] = SourceShape.of(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new TimerGraphStageLogic(shape) {
    {
      val file = directoryPath.toFile
      assert(file.exists(), s"The path: $directoryPath does not exist")
      assert(file.isDirectory, s"The path $directoryPath is not a directory")
    }

    private var buffer = Queue.empty[(Path, Change)]
    private val service: WatchService = directoryPath.getFileSystem.newWatchService()
    private val watchKey: WatchKey = directoryPath.register(
      service,
      Array[WatchEvent.Kind[_]](ENTRY_CREATE, ENTRY_MODIFY, ENTRY_DELETE),
      // this is com.sun internal, but the service is useless on OSX without it
      SensitivityWatchEventModifier.HIGH
    )

    setHandler(out, new OutHandler {
      override def onPull(): Unit = {
        if (buffer.nonEmpty) pushHead()
        else {
          poll()
          if (buffer.nonEmpty) pushHead()
          else schedulePoll()
        }
      }
    })

    def schedulePoll(): Unit = {
      scheduleOnce(true, pollInterval)
    }

    override def onTimer(any: Any): Unit = {
      if (!isClosed(out)) {
        poll()
        if (buffer.nonEmpty) pushHead()
        else schedulePoll()
      }
    }

    def poll(): Unit = {
      try {
        watchKey.pollEvents().asScala.foreach { eventt =>

          val event = eventt.asInstanceOf[WatchEvent[Path]]
          event.kind() match {
            case k if k == StandardWatchEventKinds.OVERFLOW =>
              failStage(new RuntimeException("Overflow from watch service: " + event.context()))
            case _ =>
              val path = event.context()
              val absolutePath = directoryPath.resolve(path)
              val kind = japiKindToChange(event.kind())

              buffer = buffer.enqueue((absolutePath, kind))
              if (buffer.size > maxBufferSize) failStage(new RuntimeException(s"Max event buffer size $maxBufferSize reached for $path"))
          }
        }
      } finally {
        watchKey.reset()
      }
    }

    def pushHead(): Unit = {
      val (head, rest) = buffer.dequeue
      buffer = rest
      push(out, head)
    }

    override def postStop(): Unit = {
      if (watchKey.isValid) watchKey.cancel()
      service.close()
    }
  }

}
