/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.contrib;

import akka.NotUsed;
import akka.stream.Attributes;
import akka.stream.Outlet;
import akka.stream.SourceShape;
import akka.stream.javadsl.Source;
import akka.stream.stage.*;
import akka.util.ByteString;
import scala.concurrent.duration.FiniteDuration;
import scala.util.Failure;
import scala.util.Success;
import scala.util.Try;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Read the entire contents of a file, and then when the end is reached, keep reading
 * newly appended data. Like the unix command `tail -f`.
 *
 * Aborting the stage can be done by combining with a [[akka.stream.KillSwitch]]
 */
public final class FileTailSource extends GraphStage<SourceShape<ByteString>> {

  private final Path path;
  private final int maxChunkSize;
  private final long startingPosition;
  private final FiniteDuration pollingInterval;
  private final Outlet<ByteString> out = Outlet.create("FileTailSource.out");
  private final SourceShape<ByteString> shape = SourceShape.of(out);

  // this is stateless, so can be shared among instances
  private static final CompletionHandler<Integer, AsyncCallback<Try<Integer>>> completionHandler = new CompletionHandler<Integer, AsyncCallback<Try<Integer>>>() {
    @Override
    public void completed(Integer result, AsyncCallback<Try<Integer>> attachment) {
      attachment.invoke(new Success<>(result));
    }

    @Override
    public void failed(Throwable exc, AsyncCallback<Try<Integer>> attachment) {
      attachment.invoke(new Failure<>(exc));
    }
  };

  public FileTailSource(Path path, int maxChunkSize, long startingPosition, FiniteDuration pollingInterval) {
    this.path = path;
    this.maxChunkSize = maxChunkSize;
    this.startingPosition = startingPosition;
    this.pollingInterval = pollingInterval;
  }

  @Override
  public SourceShape<ByteString> shape() {
    return shape;
  }

  @Override
  public GraphStageLogic createLogic(Attributes inheritedAttributes) {
    try {
      if (!Files.exists(path)) throw new IllegalArgumentException("Path '" + path + "' does not exist");
      if (Files.isDirectory(path)) throw new IllegalArgumentException("Path '" + path + "' cannot be tailed, it is a directory");
      if (!Files.isReadable(path)) throw new IllegalArgumentException("No read permission for '" + path + "'");

      return new TimerGraphStageLogic(shape) {
        private final ByteBuffer buffer = ByteBuffer.allocate(maxChunkSize);
        private final AsynchronousFileChannel channel = AsynchronousFileChannel.open(path, StandardOpenOption.READ);

        private long position = startingPosition;
        private AsyncCallback<Try<Integer>> chunkCallback;

        {
          setHandler(out, new AbstractOutHandler() {
            @Override
            public void onPull() throws Exception {
              doPull();
            }
          });
        }

        @Override
        public void preStart() {
          chunkCallback = createAsyncCallback((tryInteger) -> {
            if (tryInteger.isSuccess()) {
              int readBytes = tryInteger.get();
              if (readBytes > 0) {
                buffer.flip();
                push(out, ByteString.fromByteBuffer(buffer));
                position += readBytes;
                buffer.clear();
              } else {
                // hit end, try again in a while
                scheduleOnce("poll", pollingInterval);
              }

            } else {
              failStage(tryInteger.failed().get());
            }

          });
        }

        @Override
        public void onTimer(Object timerKey) {
          doPull();
        }


        private void doPull() {
          channel.read(buffer, position, chunkCallback, completionHandler);
        }

        @Override
        public void postStop() {
          try {
            if (channel.isOpen()) channel.close();
          } catch(Exception ex) {
            // Remove when #21168 is fixed
            throw new RuntimeException(ex);
          }
        }
      };

    } catch (Exception ex) {
      // remove when #21168 is fixed
      throw new RuntimeException(ex);
    }
  }


  // factory methods

  /**
   * Java API:
   *
   * Read the entire contents of a file, and then when the end is reached, keep reading
   * newly appended data. Like the unix command `tail -f`.
   *
   * Aborting the stage can be done by combining with a [[akka.stream.KillSwitch]]
   *
   * @param path a file path to tail
   * @param maxChunkSize The max emitted size of the `ByteString`s
   * @param startingPosition Offset into the file to start reading
   * @param pollingInterval When the end has been reached, look for new content with this interval
   */
  public static Source<ByteString, NotUsed> create(Path path, int maxChunkSize, long startingPosition, FiniteDuration pollingInterval) {
    return Source.fromGraph(new FileTailSource(path, maxChunkSize, startingPosition, pollingInterval));
  }

  /**
   * Scala API:
   *
   * Read the entire contents of a file, and then when the end is reached, keep reading
   * newly appended data. Like the unix command `tail -f`.
   *
   * Aborting the stage can be done by combining with a [[akka.stream.KillSwitch]]
   *
   * @param path a file path to tail
   * @param maxChunkSize The max emitted size of the `ByteString`s
   * @param startingPosition Offset into the file to start reading
   * @param pollingInterval When the end has been reached, look for new content with this interval
   */
  public static akka.stream.scaladsl.Source<ByteString, NotUsed> apply(Path path, int maxChunkSize, long startingPosition, FiniteDuration pollingInterval) {
    return create(path, maxChunkSize, startingPosition, pollingInterval).asScala();
  }

}
