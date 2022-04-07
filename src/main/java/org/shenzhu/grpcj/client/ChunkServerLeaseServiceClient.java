package org.shenzhu.grpcj.client;

import io.grpc.Channel;
import org.shenzhu.grpcj.protos.ChunkServerLeaseServiceGrpc;
import org.shenzhu.grpcj.protos.ChunkServerLeaseServiceOuterClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChunkServerLeaseServiceClient {
  private final Logger logger = LoggerFactory.getLogger(getClass());

  /** Stub to communicate with server. */
  private ChunkServerLeaseServiceGrpc.ChunkServerLeaseServiceBlockingStub blockingStub;

  /**
   * Constructor.
   *
   * @param channel grpc channel
   */
  public ChunkServerLeaseServiceClient(Channel channel) {
    this.blockingStub = ChunkServerLeaseServiceGrpc.newBlockingStub(channel);
  }

  /**
   * Send GrantLeaseRequest to chunk server.
   *
   * @param request GrantLeaseRequest
   * @return GrantLeaseReply
   */
  public ChunkServerLeaseServiceOuterClass.GrantLeaseReply grantLease(
      ChunkServerLeaseServiceOuterClass.GrantLeaseRequest request) {
    logger.info("Will try to send GrantLeaseRequest to chunk server: {}", request.toString());

    ChunkServerLeaseServiceOuterClass.GrantLeaseReply reply = this.blockingStub.grantLease(request);

    logger.info("Received GrantLeaseReply: {}", reply.toString());

    return reply;
  }

  /**
   * Send RevokeLeaseRequest to chunk server.
   *
   * @param request RevokeLeaseRequest
   * @return RevokeLeaseReply
   */
  public ChunkServerLeaseServiceOuterClass.RevokeLeaseReply revokeLease(
      ChunkServerLeaseServiceOuterClass.RevokeLeaseRequest request) {
    logger.info("Will try to send RevokeLeaseRequest to chunk server: {}", request.toString());

    ChunkServerLeaseServiceOuterClass.RevokeLeaseReply reply =
        this.blockingStub.revokeLease(request);

    logger.info("Received RevokeLeaseReply: {}", reply.toString());

    return reply;
  }
}
