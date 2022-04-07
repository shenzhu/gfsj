package org.shenzhu.grpcj.client;

import io.grpc.Channel;
import org.shenzhu.grpcj.protos.ChunkServerFileServiceGrpc;
import org.shenzhu.grpcj.protos.ChunkServerFileServiceOuterClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChunkServerFileServiceClient {
  private final Logger logger = LoggerFactory.getLogger(getClass());

  /** Stub to communicate with server. */
  private ChunkServerFileServiceGrpc.ChunkServerFileServiceBlockingStub blockingStub;

  /**
   * Constructor.
   *
   * @param channel grpc channel
   */
  public ChunkServerFileServiceClient(Channel channel) {
    this.blockingStub = ChunkServerFileServiceGrpc.newBlockingStub(channel);
  }

  /**
   * Send InitFileChunkRequest to chunk server.
   *
   * @param request InitFileChunkRequest
   * @return InitFileChunkReply
   */
  public ChunkServerFileServiceOuterClass.InitFileChunkReply initFileChunk(
      ChunkServerFileServiceOuterClass.InitFileChunkRequest request) {
    logger.info("Will try to send InitFileChunkRequest to chunk server: {}", request.toString());

    ChunkServerFileServiceOuterClass.InitFileChunkReply reply =
        this.blockingStub.initFileChunk(request);

    logger.info("Received InitFileChunkReply: {}", reply.toString());

    return reply;
  }

  /**
   * Send AdvanceFileChunkVersionReply to chunk server.
   *
   * @param request AdvanceFileChunkVersionRequest
   * @return AdvanceFileChunkVersionReply
   */
  public ChunkServerFileServiceOuterClass.AdvanceFileChunkVersionReply advanceFileChunkVersion(
      ChunkServerFileServiceOuterClass.AdvanceFileChunkVersionRequest request) {
    logger.info(
        "Will try to send AdvanceFileChunkVersionReply to chunk server: {}", request.toString());

    ChunkServerFileServiceOuterClass.AdvanceFileChunkVersionReply reply =
        this.blockingStub.advanceFileChunkVersion(request);

    logger.info("Received AdvanceFileChunkVersionReply: {}", reply.toString());

    return reply;
  }
}
