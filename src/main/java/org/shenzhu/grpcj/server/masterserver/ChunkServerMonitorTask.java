package org.shenzhu.grpcj.server.masterserver;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.shenzhu.grpcj.client.ChunkServerControlServiceClient;
import org.shenzhu.grpcj.protos.ChunkServerControlServiceOuterClass;
import org.shenzhu.grpcj.protos.ChunkServerOuterClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ChunkServerMonitorTask {
  private final Logger logger = LoggerFactory.getLogger(getClass());

  /** Thread to run monitoring task. */
  private Thread monitorThread;

  /** Boolean value indicating if monitoring task is running. */
  private final AtomicBoolean running;

  /** Mapping chunk server name to its gRPC client. */
  private final ConcurrentHashMap<String, ChunkServerControlServiceClient> chunkServerClients;

  /** Constructor. */
  public ChunkServerMonitorTask() {
    this.running = new AtomicBoolean(false);
    this.chunkServerClients = new ConcurrentHashMap<>();
  }

  /** Actual function that performs heartbeat checking for registered chunk servers. */
  private void monitorHeartBeat() {
    logger.info("Chunk server heartbeat monitor task is running in the background...");

    final int maxAttempts = 3;
    while (true) {
      if (ChunkServerManager.getInstance().getChunkServerMap().isEmpty()) {
        logger.info("No chunk server registered");
      } else {
        for (Map.Entry<ChunkServerOuterClass.ChunkServerLocation, ChunkServerOuterClass.ChunkServer>
            entry : ChunkServerManager.getInstance().getChunkServerMap().entrySet()) {
          if (isTerminateSignalled()) {
            return;
          }

          // Extract server address
          String serverHost = entry.getKey().getServerHostname();
          int port = entry.getKey().getServerPort();
          String serverAddress = serverHost + ":" + port;

          // Send request
          boolean heartBeatSucceeded = false;
          ChunkServerControlServiceClient client =
              getOrCreateChunkServerControlClient(serverAddress);
          ChunkServerControlServiceOuterClass.CheckHeartBeatRequest request =
              ChunkServerControlServiceOuterClass.CheckHeartBeatRequest.newBuilder().build();
          for (int i = 0; i < maxAttempts; ++i) {
            try {
              client.checkHeartBeat(request);

              logger.info("Received heartbeat from chunk server {}", serverAddress);
              heartBeatSucceeded = true;
              break;
            } catch (Exception exception) {
              logger.warn(
                  "Heart beat failed for chunk server {} with exception {}",
                  serverAddress,
                  exception.toString());
            }
          }

          if (!heartBeatSucceeded) {
            logger.info("Unregistering chunk server: {}", serverAddress);

            ChunkServerManager.getInstance().unRegisterChunkServer(entry.getKey());
          }
        }
      }

      if (isTerminateSignalled()) {
        return;
      }

      try {
        TimeUnit.SECONDS.sleep(5);
      } catch (InterruptedException interruptedException) {
        logger.error("Received InterruptedException: {}", interruptedException.toString());
      }
    }
  }

  /**
   * Get or create chunk server control client for given server address.
   *
   * @param serverAddress server address
   * @return ChunkServerControlClient
   */
  private ChunkServerControlServiceClient getOrCreateChunkServerControlClient(
      String serverAddress) {
    if (!this.chunkServerClients.containsKey(serverAddress)) {
      logger.info(
          "Establishing new connection to chunk server {} for heartbeat monitoring", serverAddress);

      // Create connection
      ManagedChannel channel =
          ManagedChannelBuilder.forTarget(serverAddress).usePlaintext().build();
      ChunkServerControlServiceClient client = new ChunkServerControlServiceClient(channel);

      this.chunkServerClients.put(serverAddress, client);
    }

    return this.chunkServerClients.get(serverAddress);
  }

  /**
   * Check if we should terminate monitoring.
   *
   * @return if we should terminate monitoring
   */
  private boolean isTerminateSignalled() {
    if (!this.running.get()) {
      logger.info(
          "Chunk server heartbeat monitor task received signal to terminate. Terminating...");
      return true;
    }

    return false;
  }

  /** Start monitoring. */
  public void start() {
    if (!this.running.get()) {
      this.running.set(true);

      logger.info("Starting monitoring");

      this.monitorThread =
          new Thread() {
            @Override
            public void run() {
              monitorHeartBeat();
            }
          };
      this.monitorThread.start();
    }
  }

  /** Stop monitoring. */
  public void stop() {
    if (this.running.get()) {
      this.running.set(false);

      logger.info("Stop monitoring");
    }
  }
}
