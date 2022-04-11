package org.shenzhu.grpcj.server.chunkserver;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.shenzhu.grpcj.utils.ConfigManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class ChunkServerStarter {
  private static Logger logger = LoggerFactory.getLogger(ChunkServerStarter.class);

  public static void main(String[] args) throws Exception {
    String configFilePath = "src/main/resources/yaml/sample_config.yaml";

    logger.info("Reading GFS configuration at {}", configFilePath);
    ConfigManager configManager = new ConfigManager(configFilePath);

    logger.info("Starting chunk servers");

    List<Thread> chunkServerThreads = new LinkedList<>();
    for (String chunkServerName : configManager.getAllChunkServers()) {
      logger.info("Starting chunk server {}", chunkServerName);

      // Initialize FileChunkManager
      String chunkServerDatabaseName = configManager.getDatabaseName(chunkServerName);
      int maxChunkSize = configManager.getFileChunkBlockSize();
      logger.info(
          "Initializing FileChunkManager with database {} and size {}",
          chunkServerDatabaseName,
          maxChunkSize);
      FileChunkManager.getInstance().initialize(chunkServerDatabaseName, maxChunkSize);

      // Get master server address, assume only on master server now
      String masterAddress =
          configManager.getServerAddress(configManager.getAllMasterServers().get(0));
      logger.info("Establishing connection to master server: {}", masterAddress);
      ManagedChannel masterChannel =
          ManagedChannelBuilder.forTarget(masterAddress).usePlaintext().build();

      ChunkServerImpl chunkServer =
          new ChunkServerImpl(FileChunkManager.getInstance(), masterChannel);

      Server server =
          ServerBuilder.forPort(Integer.parseInt(configManager.getServerPort(chunkServerName)))
              .addService(
                  new ChunkServerFileServiceImpl(FileChunkManager.getInstance(), chunkServer))
              .addService(new ChunkServerControlServiceImpl(chunkServer))
              .addService(new ChunkServerLeaseServiceImpl(chunkServer))
              .build();

      Thread serverThread =
          new Thread() {
            @Override
            public void run() {
              try {
                server.start();
              } catch (IOException e) {
                e.printStackTrace();
              }
            }
          };
      chunkServerThreads.add(serverThread);
      serverThread.start();

      chunkServer.startReportToMaster();
    }

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread() {
              @Override
              public void run() {
                logger.warn("Shutting down all chunk servers");

                chunkServerThreads.forEach(Thread::interrupt);
              }
            });
  }
}
