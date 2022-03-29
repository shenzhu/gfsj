package org.shenzhu.grpcj.server.chunkserver;

import org.shenzhu.grpcj.protos.ChunkServerLeaseServiceGrpc;
import org.shenzhu.grpcj.protos.ChunkServerLeaseServiceOuterClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class ChunkServerLeaseServiceImpl
    extends ChunkServerLeaseServiceGrpc.ChunkServerLeaseServiceImplBase {
  private final Logger logger = LoggerFactory.getLogger(getClass());

  /** Handle of chunk server. */
  private final ChunkServerImpl chunkServer;

  public ChunkServerLeaseServiceImpl(ChunkServerImpl chunkServer) {
    this.chunkServer = chunkServer;
  }

  @Override
  public void grantLease(
      org.shenzhu.grpcj.protos.ChunkServerLeaseServiceOuterClass.GrantLeaseRequest request,
      io.grpc.stub.StreamObserver<
              org.shenzhu.grpcj.protos.ChunkServerLeaseServiceOuterClass.GrantLeaseReply>
          responseObserver) {
    logger.info("Received GrantLeaseRequest: {}", request.toString());

    // Response builder
    ChunkServerLeaseServiceOuterClass.GrantLeaseReply.Builder replyBuilder =
        ChunkServerLeaseServiceOuterClass.GrantLeaseReply.newBuilder();
    replyBuilder.setRequest(request);

    // Validate request
    logger.info("Validating GrantLeaseRequest for: {}", request.getChunkHandle());
    Optional<Integer> storedChunkVersion =
        this.chunkServer.getChunkVersion(request.getChunkHandle());
    if (storedChunkVersion.isEmpty()) {
      // Cannot find chunk in chunk server
      logger.warn(
          "Cannot accept lease because {} doesn't exist on this chunk server",
          request.getChunkHandle());

      replyBuilder.setStatus(
          ChunkServerLeaseServiceOuterClass.GrantLeaseReply.GrantLeaseStatus.REJECTED_NOT_FOUND);
    } else if (storedChunkVersion.get() < request.getChunkVersion()) {
      // Stored chunk version is smaller than version in request
      logger.info(
          "Cannot accept lease because {} is stale on this chunk server", request.getChunkHandle());

      replyBuilder.setStatus(
          ChunkServerLeaseServiceOuterClass.GrantLeaseReply.GrantLeaseStatus
              .REJECTED_STALE_VERSION);
    } else if (storedChunkVersion.get() > request.getChunkVersion()) {
      // Stored chunk version is greater than version in request
      logger.info(
          "Cannot accept lease because {} has a newer version on this chunk server",
          request.getChunkHandle());

      replyBuilder.setStatus(
          ChunkServerLeaseServiceOuterClass.GrantLeaseReply.GrantLeaseStatus.UNKNOWN);
    } else if (System.currentTimeMillis() / 1000 > request.getLeaseExpirationTime().getSeconds()) {
      // Granted lease already expired
      logger.info(
          "Cannot accept leaase for {} because granted lease is already expired",
          request.getChunkHandle());

      replyBuilder.setStatus(
          ChunkServerLeaseServiceOuterClass.GrantLeaseReply.GrantLeaseStatus.IGNORED_EXPIRED_LEASE);
    } else {
      // Request validated, update lease
      this.chunkServer.addOrUpdateLease(
          request.getChunkHandle(), request.getLeaseExpirationTime().getSeconds());

      replyBuilder.setStatus(
          ChunkServerLeaseServiceOuterClass.GrantLeaseReply.GrantLeaseStatus.ACCEPTED);
    }

    responseObserver.onNext(replyBuilder.build());
    responseObserver.onCompleted();
  }

  @Override
  public void revokeLease(
      org.shenzhu.grpcj.protos.ChunkServerLeaseServiceOuterClass.RevokeLeaseRequest request,
      io.grpc.stub.StreamObserver<
              org.shenzhu.grpcj.protos.ChunkServerLeaseServiceOuterClass.RevokeLeaseReply>
          responseObserver) {
    logger.info("Received RevokeLeaseRequest: {}", request.toString());

    // Response builder
    ChunkServerLeaseServiceOuterClass.RevokeLeaseReply.Builder replyBuilder =
        ChunkServerLeaseServiceOuterClass.RevokeLeaseReply.newBuilder();
    replyBuilder.setRequest(request);

    // Validate request
    logger.info("Validating RevokeLeaseRequest for: {}", request.getChunkHandle());
    Optional<Long> currLeaseExpirationTime =
        this.chunkServer.getLeaseExpirationTime(request.getChunkHandle());
    if (currLeaseExpirationTime.isEmpty()) {
      logger.info("No lease to revoke for {}", request.getChunkHandle());

      replyBuilder.setStatus(
          ChunkServerLeaseServiceOuterClass.RevokeLeaseReply.RevokeLeaseStatus.REJECTED_NOT_FOUND);
    } else {

      long originalLeaseExpirationTime = request.getOriginalLeaseExpirationTime().getSeconds();
      if (originalLeaseExpirationTime < currLeaseExpirationTime.get()) {
        // Already holds a newer lease than the one trying to revoke
        logger.info(
            "Chunk server already holds a newer lease that expires in future for chunk {}",
            request.getChunkHandle());

        replyBuilder.setStatus(
            ChunkServerLeaseServiceOuterClass.RevokeLeaseReply.RevokeLeaseStatus
                .IGNORED_HAS_NEWER_LEASE);
      } else {
        // Holding lease smaller than requested, revoke lease
        this.chunkServer.removeLease(request.getChunkHandle());
        logger.info("Lease successfully revoked for {}", request.getChunkHandle());

        replyBuilder.setStatus(
            ChunkServerLeaseServiceOuterClass.RevokeLeaseReply.RevokeLeaseStatus.REVOKED);
      }
    }

    responseObserver.onNext(replyBuilder.build());
    responseObserver.onCompleted();
  }
}
