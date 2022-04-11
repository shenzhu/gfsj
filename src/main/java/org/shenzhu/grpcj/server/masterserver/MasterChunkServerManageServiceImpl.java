package org.shenzhu.grpcj.server.masterserver;

import org.shenzhu.grpcj.protos.ChunkServerManageServiceGrpc;
import org.shenzhu.grpcj.protos.ChunkServerManageServiceOuterClass;
import org.shenzhu.grpcj.protos.ChunkServerOuterClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

public class MasterChunkServerManageServiceImpl
    extends ChunkServerManageServiceGrpc.ChunkServerManageServiceImplBase {
  private final Logger logger = LoggerFactory.getLogger(getClass());

  @Override
  public void reportChunkServer(
      org.shenzhu.grpcj.protos.ChunkServerManageServiceOuterClass.ReportChunkServerRequest request,
      io.grpc.stub.StreamObserver<
              org.shenzhu.grpcj.protos.ChunkServerManageServiceOuterClass.ReportChunkServerReply>
          responseObserver) {
    ChunkServerOuterClass.ChunkServer incomingChunkServer = request.getChunkServer();
    logger.info("Master handling report from: {}", incomingChunkServer.toString());

    // Reply builder
    ChunkServerManageServiceOuterClass.ReportChunkServerReply.Builder
        reportChunkServerReplyBuilder =
            ChunkServerManageServiceOuterClass.ReportChunkServerReply.newBuilder();
    reportChunkServerReplyBuilder.setRequest(request);

    ChunkServerOuterClass.ChunkServer existingChunkServerInfo =
        ChunkServerManager.getInstance().getChunkServer(incomingChunkServer.getLocation());
    if (existingChunkServerInfo == null) {
      ChunkServerOuterClass.ChunkServer newChunkServer =
          ChunkServerOuterClass.ChunkServer.newBuilder(incomingChunkServer).build();

      ChunkServerManager.getInstance().registerChunkServer(newChunkServer);
    } else {
      Set<String> chunksToAdd = new HashSet<>(incomingChunkServer.getStoredChunkHandlesList());
      Set<String> chunksToRemove = new HashSet<>();

      for (String chunkHandle : incomingChunkServer.getStoredChunkHandlesList()) {
        if (!MetadataManager.getInstance().existFileChunkMetadata(chunkHandle)) {
          logger.info(
              "Chunk handle {} no longer existed in master's metadata, marking as staled chunk to chunk server {}",
              chunkHandle,
              incomingChunkServer.toString());

          reportChunkServerReplyBuilder.addStaleChunkHandles(chunkHandle);
          chunksToAdd.remove(chunkHandle);
        }
      }

      // Compare with stored chunk handles
      for (String storedChunkHandle : existingChunkServerInfo.getStoredChunkHandlesList()) {
        if (chunksToAdd.contains(storedChunkHandle)) {
          chunksToAdd.remove(storedChunkHandle);
        } else {
          chunksToRemove.add(storedChunkHandle);
        }
      }

      ChunkServerManager.getInstance()
          .updateChunkServer(
              incomingChunkServer.getLocation(),
              incomingChunkServer.getAvailableDiskMb(),
              chunksToAdd,
              chunksToRemove);
    }

    responseObserver.onNext(reportChunkServerReplyBuilder.build());
    responseObserver.onCompleted();
  }
}
