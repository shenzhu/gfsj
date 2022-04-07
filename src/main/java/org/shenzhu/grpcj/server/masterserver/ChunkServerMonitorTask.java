package org.shenzhu.grpcj.server.masterserver;

import org.shenzhu.grpcj.client.ChunkServerControlServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
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
}
