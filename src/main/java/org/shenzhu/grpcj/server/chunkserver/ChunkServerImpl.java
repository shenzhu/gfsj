package org.shenzhu.grpcj.server.chunkserver;

import io.grpc.Channel;
import io.grpc.StatusRuntimeException;
import org.shenzhu.grpcj.client.ChunkServerManageServiceClient;
import org.shenzhu.grpcj.protos.ChunkServerManageServiceOuterClass;
import org.shenzhu.grpcj.protos.ChunkServerOuterClass;
import org.shenzhu.grpcj.protos.Metadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class ChunkServerImpl {
  private final Logger logger = LoggerFactory.getLogger(getClass());

  /** File chunk manager. */
  private final FileChunkManager fileChunkManager;

  /** Hashmap mapping chunk handle to its version. */
  private final ConcurrentHashMap<String, Integer> chunkVersions;

  /** Hashmap mapping chunk handle to its lease expiration time. */
  private final ConcurrentHashMap<String, Long> chunkLeaseExpirationTime;

  /** Name of master server. */
  private final String masterServerName;

  /** Client to master to report chunk server status. */
  private final ChunkServerManageServiceClient chunkServerManageServiceClient;

  /** Should terminate report to master server. */
  private final AtomicBoolean terminateReporting;

  /** Thread for reporting. */
  private Thread reportingThread;

  public ChunkServerImpl(FileChunkManager fileChunkManager, Channel channel) {
    this.fileChunkManager = fileChunkManager;

    this.chunkVersions = new ConcurrentHashMap<>();
    this.chunkLeaseExpirationTime = new ConcurrentHashMap<>();

    this.masterServerName = "master";
    this.chunkServerManageServiceClient = new ChunkServerManageServiceClient(channel);

    this.terminateReporting = new AtomicBoolean(false);
  }

  // -----------------------------------------------------------------------
  // Lease management
  // -----------------------------------------------------------------------

  /**
   * Remove lease for chunk handle.
   *
   * @param chunkHandle chunk handle
   */
  public void removeLease(String chunkHandle) {
    this.chunkLeaseExpirationTime.remove(chunkHandle);
  }

  /**
   * Get lease expiration time for chunk handle.
   *
   * @param chunkHandle chunk handle
   * @return Optional of expiration time
   */
  public Optional<Long> getLeaseExpirationTime(String chunkHandle) {
    if (!this.chunkLeaseExpirationTime.containsKey(chunkHandle)) {
      return Optional.empty();
    } else {
      return Optional.of(this.chunkLeaseExpirationTime.get(chunkHandle));
    }
  }

  /**
   * Add or update lease expiration time for chunk handle.
   *
   * @param chunkHandle chunk handle
   * @param expirationTime lease expiration time
   */
  public void addOrUpdateLease(String chunkHandle, long expirationTime) {
    this.chunkLeaseExpirationTime.put(chunkHandle, expirationTime);
  }

  /**
   * Chunk if chunk server has lease for chunk handle.
   *
   * @param chunkHandle chunk handle
   * @return if has lease
   */
  public boolean hasWriteLease(String chunkHandle) {
    if (!this.chunkLeaseExpirationTime.containsKey(chunkHandle)) {
      return false;
    }

    long expirationTime = this.chunkLeaseExpirationTime.get(chunkHandle);
    return expirationTime >= System.currentTimeMillis() / 1000;
  }

  /**
   * Get the chunk version of given chunk handle.
   *
   * @param chunkHandle chunk handle
   * @return Optional of integer
   */
  public Optional<Integer> getChunkVersion(String chunkHandle) {
    return this.fileChunkManager.getChunkVersion(chunkHandle);
  }

  // -----------------------------------------------------------------------
  // Chunk server management
  // -----------------------------------------------------------------------

  /** Start reporting to master. */
  public void startReportToMaster() {
    if (this.reportingThread != null) {
      this.reportingThread =
          new Thread(
              new Runnable() {
                @Override
                public void run() {
                  while (!terminateReporting.get()) {
                    boolean reportStatus = reportToMaster();
                    if (!reportStatus) {
                      logger.error("Failed to report to master");
                    }

                    try {
                      // Sleep for several seconds before reporting again
                      Thread.sleep(3000);
                    } catch (InterruptedException interruptedException) {
                      logger.error("Failed to sleep due to: {}", interruptedException.toString());
                    }
                  }
                }
              });
    }
  }

  /** Stop reporting to master server. */
  public void terminateReportToMaster() {
    this.terminateReporting.set(false);
    if (this.reportingThread != null) {

      try {
        this.reportingThread.join();
      } catch (InterruptedException interruptedException) {
        logger.error(
            "Failed to wait for reporting thread to stop: {}", interruptedException.toString());
      }
    }
  }

  /**
   * Report chunk status in current chunk server to master server.
   *
   * @return if report succeeded.
   */
  public boolean reportToMaster() {
    // Create request builder
    ChunkServerManageServiceOuterClass.ReportChunkServerRequest.Builder requestBuilder =
        ChunkServerManageServiceOuterClass.ReportChunkServerRequest.newBuilder();

    // Build ChunkServer
    ChunkServerOuterClass.ChunkServer.Builder chunkServerBuilder =
        ChunkServerOuterClass.ChunkServer.newBuilder();
    chunkServerBuilder.setAvailableDiskMb(20 * 1024);

    // Gather information about ChunkServerLocation to set in ChunkServer
    ChunkServerOuterClass.ChunkServerLocation.Builder chunkServerLocationBuilder =
        ChunkServerOuterClass.ChunkServerLocation.newBuilder();
    chunkServerLocationBuilder.setServerHostname("localhost");
    chunkServerLocationBuilder.setServerPort(8081);
    chunkServerBuilder.setLocation(chunkServerLocationBuilder.build());

    List<Metadata.FileChunkMetadata> allChunkMetadata =
        FileChunkManager.getInstance().getAllFileChunkMetadata();

    logger.info("Found {} chunks stored to report to master", allChunkMetadata.size());
    for (int i = 0; i < allChunkMetadata.size(); ++i) {
      chunkServerBuilder.setStoredChunkHandles(i, allChunkMetadata.get(i).getChunkHandle());
    }

    // Set ChunkServer and ChunkServerLocation in request
    requestBuilder.setChunkServer(chunkServerBuilder.build());

    // Send request to all master servers
    ChunkServerManageServiceOuterClass.ReportChunkServerRequest request = requestBuilder.build();
    try {
      logger.info("Reporting to master server: {}", this.masterServerName);

      ChunkServerManageServiceOuterClass.ReportChunkServerReply reply =
          this.chunkServerManageServiceClient.reportChunkServer(request);

      // Delete stale chunk handles in current chunk server
      List<String> masterChunkHandles = reply.getStaleChunkHandlesList();
      masterChunkHandles.forEach(
          handle -> {
            logger.info("Deleting stale chunk handle: {}", handle);
            FileChunkManager.getInstance().deleteChunk(handle);
          });

      return true;
    } catch (StatusRuntimeException statusRuntimeException) {
      logger.error(
          "Failed to report to master server due to {}", statusRuntimeException.toString());

      return false;
    }
  }
}
