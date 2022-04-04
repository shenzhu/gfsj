package org.shenzhu.grpcj.client;

import io.grpc.Channel;
import io.grpc.StatusRuntimeException;
import org.shenzhu.grpcj.protos.ChunkServerManageServiceGrpc;
import org.shenzhu.grpcj.protos.ChunkServerManageServiceOuterClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChunkServerManageServiceClient {
  private final Logger logger = LoggerFactory.getLogger(getClass());

  /** Stub to communicate with server. */
  private ChunkServerManageServiceGrpc.ChunkServerManageServiceBlockingStub blockingStub;

  /**
   * Constructor.
   *
   * @param channel grpc channel
   */
  public ChunkServerManageServiceClient(Channel channel) {
    this.blockingStub = ChunkServerManageServiceGrpc.newBlockingStub(channel);
  }

  /**
   * Send reportChunkServer request to master.
   *
   * @param request ReportChunkServerRequest
   * @return ReportChunkServerReply
   * @throws StatusRuntimeException exception
   */
  public ChunkServerManageServiceOuterClass.ReportChunkServerReply reportChunkServer(
      ChunkServerManageServiceOuterClass.ReportChunkServerRequest request)
      throws StatusRuntimeException {
    logger.info("Will try to send request to master: {}", request.toString());

    ChunkServerManageServiceOuterClass.ReportChunkServerReply reply =
        this.blockingStub.reportChunkServer(request);

    logger.info("Received ReportChunkServerReply: {}", reply.toString());

    return reply;
  }
}
