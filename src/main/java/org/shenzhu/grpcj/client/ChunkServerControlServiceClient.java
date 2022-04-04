package org.shenzhu.grpcj.client;

import io.grpc.Channel;
import org.shenzhu.grpcj.protos.ChunkServerControlServiceGrpc;
import org.shenzhu.grpcj.protos.ChunkServerControlServiceOuterClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChunkServerControlServiceClient {
  private final Logger logger = LoggerFactory.getLogger(getClass());

  /** Stub to communicate with server. */
  private ChunkServerControlServiceGrpc.ChunkServerControlServiceBlockingStub blockingStub;

  /**
   * Constructor.
   *
   * @param channel grpc channel
   */
  public ChunkServerControlServiceClient(Channel channel) {
    this.blockingStub = ChunkServerControlServiceGrpc.newBlockingStub(channel);
  }

  /**
   * Send CheckHeartBeatRequest request to chunk server.
   *
   * @param request CheckHeartBeatRequest
   * @return CheckHeartBeatReply
   */
  public ChunkServerControlServiceOuterClass.CheckHeartBeatReply checkHeartBeat(
      ChunkServerControlServiceOuterClass.CheckHeartBeatRequest request) {
    logger.info("Will try to send CheckHeartBeatRequest to chunk server: {}", request.toString());

    ChunkServerControlServiceOuterClass.CheckHeartBeatReply reply =
        this.blockingStub.checkHeartBeat(request);

    logger.info("Received CheckHeartBeatReply: {}", reply.toString());

    return reply;
  }
}
