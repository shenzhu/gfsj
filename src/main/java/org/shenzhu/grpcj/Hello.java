package org.shenzhu.grpcj;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.shenzhu.grpcj.server.chunkserver.ChunkServerControlServiceImpl;
import org.shenzhu.grpcj.server.chunkserver.ChunkServerFileServiceImpl;
import org.shenzhu.grpcj.server.chunkserver.ChunkServerImpl;
import org.shenzhu.grpcj.server.chunkserver.ChunkServerLeaseServiceImpl;
import org.shenzhu.grpcj.server.chunkserver.FileChunkManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class Hello {
  static class TestChunkServer {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private Server server;

    private void start() throws IOException {
      // channel for master
      ManagedChannel channel =
          ManagedChannelBuilder.forTarget("localhost:50052").usePlaintext().build();
      ChunkServerImpl chunkServer = new ChunkServerImpl(FileChunkManager.getInstance(), channel);
      int port = 50051;

      logger.info("Creating server");

      server =
          ServerBuilder.forPort(50051)
              .addService(
                  new ChunkServerFileServiceImpl(FileChunkManager.getInstance(), chunkServer))
              .addService(new ChunkServerLeaseServiceImpl(chunkServer))
              .addService(new ChunkServerControlServiceImpl(chunkServer))
              .build()
              .start();
      logger.info("Server started, listening on {}", port);

      Runtime.getRuntime()
          .addShutdownHook(
              new Thread() {
                @Override
                public void run() {

                  // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                  System.err.println("*** shutting down gRPC server since JVM is shutting down");

                  try {
                    TestChunkServer.this.stop();
                  } catch (Exception e) {
                  }

                  System.err.println("*** server shut down");
                }
              });
    }

    private void stop() throws InterruptedException {
      if (server != null) {
        server.shutdown().awaitTermination(5, TimeUnit.SECONDS);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    System.out.println("hello world");

    String filepath = "\\foo\\bar\\baz";

    System.out.println(filepath);
    System.out.println(filepath.indexOf("x", 3));

    int delimiterPos = filepath.indexOf("\\");
    while (delimiterPos != -1) {
      String currDir = filepath.substring(0, delimiterPos);
      System.out.println(currDir);

      delimiterPos = filepath.indexOf("\\", delimiterPos + 1);
    }

    System.out.println(System.getProperty("os.name"));
    System.out.println(System.getProperty("os.name").toLowerCase().startsWith("windows"));
  }
}
