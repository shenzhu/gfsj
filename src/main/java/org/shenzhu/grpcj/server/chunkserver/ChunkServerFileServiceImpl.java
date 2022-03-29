package org.shenzhu.grpcj.server.chunkserver;

import org.shenzhu.grpcj.protos.ChunkServerFileServiceGrpc;
import org.shenzhu.grpcj.protos.ChunkServerFileServiceOuterClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

public class ChunkServerFileServiceImpl
    extends ChunkServerFileServiceGrpc.ChunkServerFileServiceImplBase {
  private final Logger logger = LoggerFactory.getLogger(getClass());

  /** File chunk manager to manage chunks in disk. */
  private final FileChunkManager fileChunkManager;

  /**
   * Constructor
   *
   * @param fileChunkManager FileChunkManager
   */
  ChunkServerFileServiceImpl(FileChunkManager fileChunkManager) {
    this.fileChunkManager = fileChunkManager;
  }

  /**
   * Internal function to write file chunk specified in requestHeader, set status in replyBuilder.
   *
   * @param requestHeader request header
   * @param replyBuilder reply builder
   * @return if write succeeded
   */
  private boolean writeFileChunkInternal(
      ChunkServerFileServiceOuterClass.WriteFileChunkRequestHeader requestHeader,
      ChunkServerFileServiceOuterClass.WriteFileChunkReply.Builder replyBuilder) {
    logger.info(
        "Checking data in cache for checksum: {}, chunk handle: {}",
        requestHeader.getDataChecksum(),
        requestHeader.getChunkHandle());

    // Try to find data in cache
    ChunkDataCacheManager cacheManager = ChunkDataCacheManager.getInstance();
    String data =
        cacheManager.getValue(requestHeader.getDataChecksum().toString(StandardCharsets.UTF_8));
    if (data == null) {
      logger.error(
          "Data not found in cache for checksum: {}, chunk handle: {}",
          requestHeader.getDataChecksum(),
          requestHeader.getChunkHandle());

      replyBuilder.setStatus(
          ChunkServerFileServiceOuterClass.FileChunkMutationStatus.FAILED_DATA_NOT_FOUND);
      return false;
    }

    logger.info(
        "Data found in cache for checksum: {}, chunk handle: {}. Now writing data to file chunk.",
        requestHeader.getDataChecksum(),
        requestHeader.getChunkHandle());

    // Do the actual disk write
    int writeLength =
        fileChunkManager.writeToChunk(
            requestHeader.getChunkHandle(),
            requestHeader.getChunkVersion(),
            requestHeader.getOffsetStart(),
            requestHeader.getLength(),
            data);
    if (writeLength > 0) {
      // Write successful
      logger.info(
          "Write success for file chunk: {}, bytes written: {}",
          requestHeader.getChunkHandle(),
          writeLength);

      replyBuilder.setBytesWritten(writeLength);
      replyBuilder.setStatus(ChunkServerFileServiceOuterClass.FileChunkMutationStatus.OK);
    } else {
      // Write failed
      logger.error("Write failed for file chunk: {}", requestHeader.getChunkHandle());

      replyBuilder.setBytesWritten(0);
      replyBuilder.setStatus(ChunkServerFileServiceOuterClass.FileChunkMutationStatus.UNKNOWN);
    }

    return false;
  }

  @Override
  public void initFileChunk(
      org.shenzhu.grpcj.protos.ChunkServerFileServiceOuterClass.InitFileChunkRequest request,
      io.grpc.stub.StreamObserver<
              org.shenzhu.grpcj.protos.ChunkServerFileServiceOuterClass.InitFileChunkReply>
          responseObserver) {
    logger.info("Received InitFileChunkRequest: {}", request.toString());

    // Status in reply response
    ChunkServerFileServiceOuterClass.InitFileChunkReply.InitFileChunkStatus replyStatus;

    // Try to create chunk with given chunk handle
    logger.info("Trying to create file chunk with version 1: {}", request.getChunkHandle());
    FileChunkManager.ChunkCreationStatus creationStatus =
        this.fileChunkManager.createChunk(request.getChunkHandle(), 1);
    if (creationStatus == FileChunkManager.ChunkCreationStatus.ALREADY_EXISTS) {
      logger.info(
          "Cannot initialize file chunk because it already exists: {}", request.getChunkHandle());
      replyStatus =
          ChunkServerFileServiceOuterClass.InitFileChunkReply.InitFileChunkStatus.ALREADY_EXISTS;
    } else if (creationStatus == FileChunkManager.ChunkCreationStatus.OK) {
      logger.info("Initial empty file chunk successfully created: {}", request.getChunkHandle());
      replyStatus = ChunkServerFileServiceOuterClass.InitFileChunkReply.InitFileChunkStatus.CREATED;
    } else {
      logger.info("Encounter unexpected error while creating file chunk");
      replyStatus = ChunkServerFileServiceOuterClass.InitFileChunkReply.InitFileChunkStatus.UNKNOWN;
    }

    // Build response
    ChunkServerFileServiceOuterClass.InitFileChunkReply reply =
        ChunkServerFileServiceOuterClass.InitFileChunkReply.newBuilder()
            .setRequest(request)
            .setStatus(replyStatus)
            .build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

  @Override
  public void writeFileChunk(
      org.shenzhu.grpcj.protos.ChunkServerFileServiceOuterClass.WriteFileChunkRequest request,
      io.grpc.stub.StreamObserver<
              org.shenzhu.grpcj.protos.ChunkServerFileServiceOuterClass.WriteFileChunkReply>
          responseObserver) {
    logger.info("Received WriteFileChunkRequest: {}", request.toString());
  }
}
