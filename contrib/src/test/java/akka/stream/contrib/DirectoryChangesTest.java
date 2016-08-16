/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.contrib;

import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.javadsl.Sink;
import akka.stream.testkit.TestSubscriber;
import akka.testkit.TestKit;
import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import com.google.common.jimfs.WatchServiceConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;

import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

public class DirectoryChangesTest {

  private ActorSystem system;
  private Materializer materializer;
  private FileSystem fs;
  private Path testDir;

  @Before
  public void setup() throws Exception {
    system = ActorSystem.create();
    materializer = ActorMaterializer.create(system);

    fs = Jimfs.newFileSystem(
      Configuration.forCurrentPlatform()
        .toBuilder()
        .setWatchServiceConfiguration(WatchServiceConfiguration.polling(10, TimeUnit.MILLISECONDS))
        .build()
    );

    testDir = fs.getPath("testdir");

    Files.createDirectory(testDir);
  }


  @Test
  public void sourceShouldEmitOnDirectoryChanges() throws Exception {
    final TestSubscriber.Probe<Pair<Path, DirectoryChanges.Change>> probe = TestSubscriber.probe(system);

    DirectoryChanges.create(testDir, FiniteDuration.create(250, TimeUnit.MILLISECONDS), 200)
      .runWith(Sink.fromSubscriber(probe), materializer);

    probe.request(1);

    // race here, since we don't know if the request reaches the stage before
    // we create the file
    Thread.sleep(250);

    final Path createdFile = Files.createFile(testDir.resolve("test1file1.sample"));

    final Pair<Path, DirectoryChanges.Change> pair1 = probe.expectNext();
    assertEquals(pair1.second(), DirectoryChanges.Change.Creation);
    assertEquals(pair1.first(), createdFile);

    Files.write(createdFile, "Some data".getBytes());

    probe.request(1);
    final Pair<Path, DirectoryChanges.Change> pair2 = probe.expectNext();
    assertEquals(pair2.second(), DirectoryChanges.Change.Modification);
    assertEquals(pair2.first(), createdFile);

    Files.delete(createdFile);

    probe.request(1);
    final Pair<Path, DirectoryChanges.Change> pair3 = probe.expectNext();
    assertEquals(pair3.second(), DirectoryChanges.Change.Deletion);
    assertEquals(pair3.first(), createdFile);

    probe.cancel();
  }


  @Test
  public void emitMultipleChanges() throws Exception {
    final TestSubscriber.Probe<Pair<Path, DirectoryChanges.Change>> probe =
      TestSubscriber.<Pair<Path, DirectoryChanges.Change>>probe(system);

    final int numberOfChanges = 50;

    DirectoryChanges.create(
      testDir,
      FiniteDuration.create(250, TimeUnit.MILLISECONDS),
      numberOfChanges * 2
    ).runWith(Sink.fromSubscriber(probe), materializer);


    probe.request(numberOfChanges);

    // race here, since we don't know if the request reaches the stage before
    // we create the file
    Thread.sleep(100);

    final int halfRequested = numberOfChanges / 2;
    final List<Path> files = new ArrayList<>();

    for (int i = 0; i < halfRequested; i++) {
      final Path file = Files.createFile(testDir.resolve("test2files" + i));
      files.add(file);
    }

    for (int i = 0; i < halfRequested; i++) {
      probe.expectNext();
    }

    for (int i = 0; i < halfRequested; i++) {
      Files.delete(files.get(i));
    }

    for (int i = 0; i < halfRequested; i++) {
      probe.expectNext();
    }

    probe.cancel();
  }

  @After
  public void tearDown() throws Exception {
    TestKit.shutdownActorSystem(system, Duration.create("20 seconds"), true);
    fs.close();
  }

}
