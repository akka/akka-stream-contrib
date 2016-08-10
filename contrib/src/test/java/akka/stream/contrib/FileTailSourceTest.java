/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.contrib;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.javadsl.Framing;
import akka.stream.javadsl.Source;
import akka.util.ByteString;
import org.junit.Test;
import scala.concurrent.duration.FiniteDuration;

import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class FileTailSourceTest {

  @Test
  public void canReadAnEntireFile() {
    fail("test missing");
  }

  @Test
  public void willReadNewLinesAppendedAfterReadingTheInitialContents() {
    fail("test missing");
  }


  public static void main(String... args) {
    if(args.length != 1) throw new IllegalArgumentException("Usage: FileTailSourceTest [path]");

    // small sample of usage, tails the first argument path
    ActorSystem system = ActorSystem.create();
    Materializer materializer = ActorMaterializer.create(system);

    FileSystem fs = FileSystems.getDefault();
    Source<ByteString, NotUsed> source = FileTailSource.create(
      fs.getPath(args[0]),
      8192, // chunk size
      0, // starting position
      FiniteDuration.create(250, TimeUnit.MILLISECONDS));

    source.via(Framing.delimiter(ByteString.fromString("\n"), 8192))
      .map(bytes -> bytes.utf8String())
      .runForeach((line) -> System.out.println(line), materializer);

  }

}
