package org.shenzhu.grpcj.server.chunkserver;

import com.google.protobuf.ByteString;
import org.shenzhu.grpcj.protos.ChunkServerFileServiceGrpc;
import org.shenzhu.grpcj.protos.ChunkServerFileServiceOuterClass;
import org.shenzhu.grpcj.utils.Checksum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;

public class ChunkServerFileServiceImpl
    extends ChunkServerFileServiceGrpc.ChunkServerFileServiceImplBase {
  private final Logger logger = LoggerFactory.getLogger(getClass());

  /** File chunk manager to manage chunks in disk. */
  private final FileChunkManager fileChunkManager;

  /** Chunk server implementation. */
  private final ChunkServerImpl chunkServerImpl;

  /**
   * Constructor
   *
   * @param fileChunkManager FileChunkManager
   */
  public ChunkServerFileServiceImpl(
      FileChunkManager fileChunkManager, ChunkServerImpl chunkServerImpl) {
    this.fileChunkManager = fileChunkManager;
    this.chunkServerImpl = chunkServerImpl;
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
  public void readFileChunk(
      org.shenzhu.grpcj.protos.ChunkServerFileServiceOuterClass.ReadFileChunkRequest request,
      io.grpc.stub.StreamObserver<
              org.shenzhu.grpcj.protos.ChunkServerFileServiceOuterClass.ReadFileChunkReply>
          responseObserver) {
    logger.info("Received ReadFileChunkRequest: {}", request.toString());

    // Reply builder
    ChunkServerFileServiceOuterClass.ReadFileChunkReply.Builder replyBuilder =
        ChunkServerFileServiceOuterClass.ReadFileChunkReply.newBuilder();
    replyBuilder.setRequest(request);

    logger.info(
        "Trying to read file chunk {} of version {} from offset {} for {} bytes",
        request.getChunkHandle(),
        request.getChunkVersion(),
        request.getOffsetStart(),
        request.getLength());

    byte[] readData =
        this.fileChunkManager.readFromChunk(
            request.getChunkHandle(),
            request.getChunkVersion(),
            request.getOffsetStart(),
            request.getLength());
    if (readData == null) {
      logger.error("Failed to read from chunk");

      replyBuilder.setStatus(
          ChunkServerFileServiceOuterClass.ReadFileChunkReply.ReadFileChunkStatus.UNKNOWN);
    } else {
      replyBuilder.setStatus(
          ChunkServerFileServiceOuterClass.ReadFileChunkReply.ReadFileChunkStatus.OK);
      replyBuilder.setData(ByteString.copyFrom(readData));
      replyBuilder.setBytesRead(readData.length);
    }

    responseObserver.onNext(replyBuilder.build());
    responseObserver.onCompleted();
  }

  @Override
  public void advanceFileChunkVersion(
      org.shenzhu.grpcj.protos.ChunkServerFileServiceOuterClass.AdvanceFileChunkVersionRequest
          request,
      io.grpc.stub.StreamObserver<
              org.shenzhu.grpcj.protos.ChunkServerFileServiceOuterClass
                  .AdvanceFileChunkVersionReply>
          responseObserver) {
    logger.info("Received AdvanceFileChunkVersion: {}", request.toString());

    // Reply builder
    ChunkServerFileServiceOuterClass.AdvanceFileChunkVersionReply.Builder replyBuilder =
        ChunkServerFileServiceOuterClass.AdvanceFileChunkVersionReply.newBuilder();
    replyBuilder.setRequest(request);

    // Get previous version
    final int fromVersion = request.getNewChunkVersion() - 1;
    logger.info(
        "Trying to advance the version of file chunk {} from version {} to version {}",
        request.getChunkHandle(),
        fromVersion,
        request.getNewChunkVersion());

    // Update version
    boolean updateStatus =
        this.fileChunkManager.updateChunkVersion(
            request.getChunkHandle(), fromVersion, request.getNewChunkVersion());
    if (!updateStatus) {
      logger.error("Found error while advancing chunk version");

      replyBuilder.setStatus(
          ChunkServerFileServiceOuterClass.AdvanceFileChunkVersionReply
              .AdvanceFileChunkVersionStatus.UNKNOWN);
    } else {
      replyBuilder.setStatus(
          ChunkServerFileServiceOuterClass.AdvanceFileChunkVersionReply
              .AdvanceFileChunkVersionStatus.OK);
      replyBuilder.setChunkVersion(request.getNewChunkVersion());
    }

    responseObserver.onNext(replyBuilder.build());
    responseObserver.onCompleted();
  }

  @Override
  public void sendChunkData(
      org.shenzhu.grpcj.protos.ChunkServerFileServiceOuterClass.SendChunkDataRequest request,
      io.grpc.stub.StreamObserver<
              org.shenzhu.grpcj.protos.ChunkServerFileServiceOuterClass.SendChunkDataReply>
          responseObserver) {
    logger.info("Received SendChunkDataRequest: {}", request.toString());

    // Reply builder
    ChunkServerFileServiceOuterClass.SendChunkDataReply.Builder replyBuilder =
        ChunkServerFileServiceOuterClass.SendChunkDataReply.newBuilder();

    // Check if data size in request is greater than allowed
    if (request.getData().size() > 100000) {
      logger.error(
          "Received chunk data with checksum {} and size {} bigger than max allowed size {}",
          request.getChecksum(),
          request.getData().size(),
          100000);

      replyBuilder.setStatus(
          ChunkServerFileServiceOuterClass.SendChunkDataReply.SendChunkDataRequestStatus
              .DATA_TOO_BIG);
    } else {
      // Get checksum and compare
      try {
        String checksum =
            new String(
                Checksum.getCheckSum(request.getData().toByteArray()), StandardCharsets.UTF_8);
        if (!checksum.equals(request.getChecksum().toString(StandardCharsets.UTF_8))) {
          logger.error(
              "Received bad chunk data. Received checksum: {}, calculated checksum: {}",
              request.getChecksum().toString(StandardCharsets.UTF_8),
              checksum);

          replyBuilder.setStatus(
              ChunkServerFileServiceOuterClass.SendChunkDataReply.SendChunkDataRequestStatus
                  .BAD_DATA);
        } else {
          ChunkDataCacheManager.getInstance()
              .setValue(
                  request.getChecksum().toString(StandardCharsets.UTF_8),
                  request.getData().toString(StandardCharsets.UTF_8));

          logger.info(
              "Received chunk data with checksum {} and size {}, will store in cache",
              request.getChecksum().toString(StandardCharsets.UTF_8),
              request.getData().size());

          replyBuilder.setStatus(
              ChunkServerFileServiceOuterClass.SendChunkDataReply.SendChunkDataRequestStatus.OK);
        }

      } catch (NoSuchAlgorithmException noSuchAlgorithmException) {
        logger.error("Failed to calculate checksum for data in request");

        replyBuilder.setStatus(
            ChunkServerFileServiceOuterClass.SendChunkDataReply.SendChunkDataRequestStatus.UNKNOWN);
      }
    }

    responseObserver.onNext(replyBuilder.build());
    responseObserver.onCompleted();
  }

  @Override
  public void writeFileChunk(
      org.shenzhu.grpcj.protos.ChunkServerFileServiceOuterClass.WriteFileChunkRequest request,
      io.grpc.stub.StreamObserver<
              org.shenzhu.grpcj.protos.ChunkServerFileServiceOuterClass.WriteFileChunkReply>
          responseObserver) {
    logger.info("Received WriteFileChunkRequest: {}", request.toString());

    // Reply builder
    ChunkServerFileServiceOuterClass.WriteFileChunkReply.Builder replyBuilder =
        ChunkServerFileServiceOuterClass.WriteFileChunkReply.newBuilder();
    replyBuilder.setRequest(request);

    ChunkServerFileServiceOuterClass.WriteFileChunkRequestHeader requestHeader =
        request.getHeader();

    // Check if we have lease present
    logger.info("Checking if we have lease on chunk handle {}", requestHeader.getChunkHandle());
    if (!this.chunkServerImpl.hasWriteLease(requestHeader.getChunkHandle())) {
      logger.error("No write lease for chunk handle {}", requestHeader.getChunkHandle());

      replyBuilder.setStatus(
          ChunkServerFileServiceOuterClass.FileChunkMutationStatus.FAILED_NOT_LEASE_HOLDER);

      responseObserver.onNext(replyBuilder.build());
      responseObserver.onCompleted();

      return;
    }

    logger.info("Found valid lease for chunk handle {}", requestHeader.getChunkHandle());

    // Write internally
    boolean internalWriteStatus = writeFileChunkInternal(requestHeader, replyBuilder);
    if (!internalWriteStatus) {
      responseObserver.onNext(replyBuilder.build());
      responseObserver.onCompleted();

      return;
    }

    // TODO: write to replicas
    logger.info(
        "Sending apply mutation request to {} replicas for chunk {}",
        request.getReplicaLocationsCount(),
        requestHeader.getChunkHandle());

    replyBuilder.setStatus(ChunkServerFileServiceOuterClass.FileChunkMutationStatus.OK);

    responseObserver.onNext(replyBuilder.build());
    responseObserver.onCompleted();
  }

  @Override
  public void applyMutations(
      org.shenzhu.grpcj.protos.ChunkServerFileServiceOuterClass.ApplyMutationsRequest request,
      io.grpc.stub.StreamObserver<
              org.shenzhu.grpcj.protos.ChunkServerFileServiceOuterClass.ApplyMutationsReply>
          responseObserver) {
    logger.info("Received ApplyMutationsRequest: {}", request.toString());

    // Reply builder
    ChunkServerFileServiceOuterClass.ApplyMutationsReply.Builder replyBuilder =
        ChunkServerFileServiceOuterClass.ApplyMutationsReply.newBuilder();
    replyBuilder.setRequest(request);

    // Get header and write internally
    ChunkServerFileServiceOuterClass.WriteFileChunkRequestHeader requestHeader =
        request.getHeaders(0);
    ChunkServerFileServiceOuterClass.WriteFileChunkReply.Builder writeReplyBuilder =
        ChunkServerFileServiceOuterClass.WriteFileChunkReply.newBuilder();
    writeFileChunkInternal(requestHeader, writeReplyBuilder);

    replyBuilder.setStatus(writeReplyBuilder.getStatus());

    responseObserver.onNext(replyBuilder.build());
    responseObserver.onCompleted();
  }
}
