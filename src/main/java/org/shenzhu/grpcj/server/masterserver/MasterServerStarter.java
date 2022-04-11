package org.shenzhu.grpcj.server.masterserver;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.shenzhu.grpcj.utils.ConfigManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MasterServerStarter {
  private static Logger logger = LoggerFactory.getLogger(MasterServerStarter.class);

  public static void main(String[] args) throws Exception {
    String configFilePath = "src/main/resources/yaml/sample_config.yaml";

    logger.info("Reading GFS configuration at {}", configFilePath);
    ConfigManager configManager = new ConfigManager(configFilePath);

    String masterServerHost =
        configManager.getServerHostName(configManager.getAllMasterServers().get(0));
    String masterServerPort =
        configManager.getServerPort(configManager.getAllMasterServers().get(0));
    logger.info("Starting master server at {}:{}", masterServerHost, masterServerPort);

    Server server =
        ServerBuilder.forPort(Integer.parseInt(masterServerPort))
            .addService(new MasterMetadataServiceImpl())
            .addService(new MasterChunkServerManageServiceImpl())
            .build();

    ChunkServerMonitorTask task = new ChunkServerMonitorTask();
    task.start();

    server.start();

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread() {
              @Override
              public void run() {
                logger.warn("Shutting down master server");

                task.stop();
                server.shutdown();
              }
            });
  }
}
