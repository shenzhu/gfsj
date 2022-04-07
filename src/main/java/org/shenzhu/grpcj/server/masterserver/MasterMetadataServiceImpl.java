package org.shenzhu.grpcj.server.masterserver;

import com.google.protobuf.Timestamp;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.shenzhu.grpcj.client.ChunkServerFileServiceClient;
import org.shenzhu.grpcj.client.ChunkServerLeaseServiceClient;
import org.shenzhu.grpcj.protos.ChunkServerFileServiceOuterClass;
import org.shenzhu.grpcj.protos.ChunkServerLeaseServiceOuterClass;
import org.shenzhu.grpcj.protos.ChunkServerOuterClass;
import org.shenzhu.grpcj.protos.MasterMetadataServiceGrpc;
import org.shenzhu.grpcj.protos.MasterMetadataServiceOuterClass;
import org.shenzhu.grpcj.protos.Metadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class MasterMetadataServiceImpl
    extends MasterMetadataServiceGrpc.MasterMetadataServiceImplBase {
  private final Logger logger = LoggerFactory.getLogger(getClass());

  /** Thread-safe hashmap mapping chunk server address to ChunkServerFileServiceClient. */
  private ConcurrentHashMap<String, ChunkServerFileServiceClient> chunkServerFileServiceClients;

  /** Thread-safe hashmap mapping chunk server address to ChunkServerLeaseServiceClient. */
  private ConcurrentHashMap<String, ChunkServerLeaseServiceClient> chunkServerLeaseServiceClients;

  /** Constructor. */
  public MasterMetadataServiceImpl() {
    this.chunkServerFileServiceClients = new ConcurrentHashMap<>();
    this.chunkServerLeaseServiceClients = new ConcurrentHashMap<>();
  }

  /**
   * Get metadata manager instance.
   *
   * @return metadata manager
   */
  private MetadataManager getMetadataManager() {
    return MetadataManager.getInstance();
  }

  /**
   * Get chunk server manager instance.
   *
   * @return chunk server manager
   */
  private ChunkServerManager getChunkServerManager() {
    return ChunkServerManager.getInstance();
  }

  /**
   * Get or create a new ChunkServerFileServiceClient to given server address.
   *
   * @param serverAddress server address
   * @return ChunkServerFileServiceClient
   */
  private ChunkServerFileServiceClient getOrCreateChunkServerFileServiceClient(
      String serverAddress) {
    if (!this.chunkServerFileServiceClients.containsKey(serverAddress)) {
      logger.info(
          "Establishing new connection to chunk server at {}, creating ChunkServerFileServiceClient",
          serverAddress);

      ManagedChannel channel =
          ManagedChannelBuilder.forTarget(serverAddress).usePlaintext().build();
      ChunkServerFileServiceClient client = new ChunkServerFileServiceClient(channel);

      this.chunkServerFileServiceClients.put(serverAddress, client);
    }

    return this.chunkServerFileServiceClients.get(serverAddress);
  }

  /**
   * Get or create a new ChunkServerLeaseServiceClient to given server address.
   *
   * @param serverAddress server address
   * @return ChunkServerLeaseServiceClient
   */
  private ChunkServerLeaseServiceClient getOrCreateChunkServerLeaseServiceClient(
      String serverAddress) {
    if (!this.chunkServerLeaseServiceClients.containsKey(serverAddress)) {
      logger.info(
          "Establishing new connection to chunk server at {}, creating ChunkServerLeaseServiceClient",
          serverAddress);

      ManagedChannel channel =
          ManagedChannelBuilder.forTarget(serverAddress).usePlaintext().build();
      ChunkServerLeaseServiceClient client = new ChunkServerLeaseServiceClient(channel);

      this.chunkServerLeaseServiceClients.put(serverAddress, client);
    }

    return this.chunkServerLeaseServiceClients.get(serverAddress);
  }

  /**
   * Handle file chunk creation.
   *
   * @param request OpenFileRequest
   * @param replyBuilder replyBuilder
   * @return if chunk creation succeeded
   */
  private boolean handleFileChunkCreation(
      MasterMetadataServiceOuterClass.OpenFileRequest request,
      MasterMetadataServiceOuterClass.OpenFileReply.Builder replyBuilder) {
    // Build reply
    replyBuilder.setRequest(request);

    final String fileName = request.getFilename();
    final int chunkIndex = request.getChunkIndex();

    // Check if file exists
    if (!getMetadataManager().existFileMeta(fileName)) {
      logger.error(
          "Cannot create file chunk index {} because file {} doesn't exist", chunkIndex, fileName);

      replyBuilder.setStatus(
          MasterMetadataServiceOuterClass.OpenFileReply.OpenFileReplyStatusCode.NOT_FOUND);
      return false;
    }

    // Step 1. Create file chunk handle
    Optional<String> chunkHandleOptional =
        getMetadataManager().createChunkHandle(fileName, chunkIndex);
    if (chunkHandleOptional.isEmpty()) {
      logger.error("Failed to create chunk handle");

      replyBuilder.setStatus(
          MasterMetadataServiceOuterClass.OpenFileReply.OpenFileReplyStatusCode.FAILED);
      return false;
    }

    String chunkHandle = chunkHandleOptional.get();
    logger.info("Chunk handle created {} for file {}", chunkHandle, fileName);

    // Step 2. Allocate chunk servers for this file chunk
    final int numOfChunkReplica = 1;
    List<ChunkServerOuterClass.ChunkServerLocation> chunkServerLocations =
        getChunkServerManager().allocateChunkServer(chunkHandle, numOfChunkReplica);
    if (chunkServerLocations.isEmpty()) {
      logger.info("Failed to allocate chunk servers for chunk {}", chunkHandle);

      replyBuilder.setStatus(
          MasterMetadataServiceOuterClass.OpenFileReply.OpenFileReplyStatusCode.FAILED);
      return false;
    }

    // Build FileChunkMetadata
    Metadata.FileChunkMetadata.Builder fileChunkMetadataBuilder =
        Metadata.FileChunkMetadata.newBuilder();
    fileChunkMetadataBuilder.setChunkHandle(chunkHandle);
    fileChunkMetadataBuilder.setVersion(1);

    // Step 3. Ask chunk servers to initialize file chunk
    boolean primaryLocationSet = false;
    for (ChunkServerOuterClass.ChunkServerLocation chunkServerLocation : chunkServerLocations) {
      // Compose server address
      String serverHostname = chunkServerLocation.getServerHostname();
      int serverPort = chunkServerLocation.getServerPort();
      String serverAddress = serverHostname + ":" + serverPort;

      // Get client
      ChunkServerFileServiceClient client = getOrCreateChunkServerFileServiceClient(serverAddress);

      // Prepare request and send
      ChunkServerFileServiceOuterClass.InitFileChunkRequest.Builder builder =
          ChunkServerFileServiceOuterClass.InitFileChunkRequest.newBuilder();
      builder.setChunkHandle(chunkHandle);

      ChunkServerFileServiceOuterClass.InitFileChunkReply reply =
          client.initFileChunk(builder.build());
      if (reply.getStatus()
          == ChunkServerFileServiceOuterClass.InitFileChunkReply.InitFileChunkStatus.UNKNOWN) {
        logger.error(
            "InitFileChunkRequest for {} sent to {} failed: {}", chunkHandle, serverAddress, reply);

        replyBuilder.setStatus(
            MasterMetadataServiceOuterClass.OpenFileReply.OpenFileReplyStatusCode.FAILED);
        return false;
      }

      logger.info("InitFileChunkRequest for {} sent to {} succeeded", chunkHandle, serverAddress);
      fileChunkMetadataBuilder.addLocations(chunkServerLocation);

      // Set primary location if not set
      if (!primaryLocationSet) {
        fileChunkMetadataBuilder.setPrimaryLocation(
            ChunkServerOuterClass.ChunkServerLocation.newBuilder()
                .setServerHostname(serverHostname)
                .setServerPort(serverPort)
                .build());
        primaryLocationSet = true;
      }
    }

    Metadata.FileChunkMetadata fileChunkMetadata = fileChunkMetadataBuilder.build();
    getMetadataManager().setFileChunkMetadata(fileChunkMetadata);

    replyBuilder.setMetadata(fileChunkMetadata);
    replyBuilder.setStatus(
        MasterMetadataServiceOuterClass.OpenFileReply.OpenFileReplyStatusCode.OK);

    return true;
  }

  /**
   * Handle file creation.
   *
   * @param request OpenFileRequest
   * @param replyBuilder reply builder
   * @return if file creation succeeded
   */
  public boolean handleFileCreation(
      MasterMetadataServiceOuterClass.OpenFileRequest request,
      MasterMetadataServiceOuterClass.OpenFileReply.Builder replyBuilder) {
    // Step 1. Create file metadata
    final String fileName = request.getFilename();
    logger.info("Handling file creation with {}", fileName);

    if (getMetadataManager().existFileMeta(fileName)) {
      logger.warn("File {} already exists", fileName);

      replyBuilder.setStatus(
          MasterMetadataServiceOuterClass.OpenFileReply.OpenFileReplyStatusCode.ALREADY_EXISTS);
      return false;
    }

    // Step 2. Create metadata for file
    MetadataManager.MetadataCreateStatus status = getMetadataManager().createFileMetadata(fileName);
    if (status == MetadataManager.MetadataCreateStatus.FAILED) {
      logger.error("Failed to create metadata for file {}", fileName);

      replyBuilder.setStatus(
          MasterMetadataServiceOuterClass.OpenFileReply.OpenFileReplyStatusCode.FAILED);
      return false;
    }

    // Step 2. Create first file chunk and allocate chunk server
    boolean chunkCreationStatus = handleFileChunkCreation(request, replyBuilder);
    if (!chunkCreationStatus) {
      logger.error("Failed to create chunk in file {}", fileName);

      getMetadataManager().deleteFileAndChunkMetadata(fileName);
      return false;
    }

    return true;
  }

  /**
   * Handle file chunk read.
   *
   * @param request OpenFileRequest
   * @param replyBuilder OpenFileReply.Builder
   * @return if file chunk read succeeded
   */
  private boolean handleFileChunkRead(
      MasterMetadataServiceOuterClass.OpenFileRequest request,
      MasterMetadataServiceOuterClass.OpenFileReply.Builder replyBuilder) {
    // Step 1. Access chunk handle
    final String fileName = request.getFilename();
    final int chunkIndex = request.getChunkIndex();
    logger.info("Handling read for file {} at index {}", fileName, chunkIndex);

    if (!getMetadataManager().existFileMeta(fileName)) {
      logger.error("Cannot read file {} because it doesn't exist", fileName);

      replyBuilder.setStatus(
          MasterMetadataServiceOuterClass.OpenFileReply.OpenFileReplyStatusCode.NOT_FOUND);
      return false;
    }

    Optional<String> chunkHandleOptional =
        getMetadataManager().getChunkHandle(fileName, chunkIndex);
    if (chunkHandleOptional.isEmpty()) {
      logger.error("Failed to get chunk handle in file {} at index {}", fileName, chunkIndex);

      replyBuilder.setStatus(
          MasterMetadataServiceOuterClass.OpenFileReply.OpenFileReplyStatusCode.FAILED);
      return false;
    }

    // Step 2. Access file chunk metadata
    String chunkHandle = chunkHandleOptional.get();
    Optional<Metadata.FileChunkMetadata> fileChunkMetadataOptional =
        getMetadataManager().getFileChunkMetadata(chunkHandle);
    if (fileChunkMetadataOptional.isEmpty()) {
      logger.info("Cannot access file chunk metadata for chunk handle {}", chunkHandle);

      replyBuilder.setStatus(
          MasterMetadataServiceOuterClass.OpenFileReply.OpenFileReplyStatusCode.FAILED);
      return false;
    }

    Metadata.FileChunkMetadata fileChunkMetadata = fileChunkMetadataOptional.get();

    Metadata.FileChunkMetadata.Builder newFileChunkMetadataBuilder =
        Metadata.FileChunkMetadata.newBuilder();
    newFileChunkMetadataBuilder.setChunkHandle(chunkHandle);
    newFileChunkMetadataBuilder.setVersion(fileChunkMetadata.getVersion());
    for (ChunkServerOuterClass.ChunkServerLocation chunkServerLocation :
        getChunkServerManager().getChunkLocations(chunkHandle)) {
      newFileChunkMetadataBuilder.addLocations(chunkServerLocation);
    }
    if (newFileChunkMetadataBuilder.getLocationsCount() == 0) {
      logger.error("No chunk servers available right now for read");
      replyBuilder.setStatus(
          MasterMetadataServiceOuterClass.OpenFileReply.OpenFileReplyStatusCode.UNAVAILABLE);

      return false;
    }

    replyBuilder.setMetadata(newFileChunkMetadataBuilder.build());
    return true;
  }

  private boolean handleFileChunkWrite(
      MasterMetadataServiceOuterClass.OpenFileRequest request,
      MasterMetadataServiceOuterClass.OpenFileReply.Builder replyBuilder) {
    // Step 1. Access chunk handle
    final String fileName = request.getFilename();
    final int chunkIndex = request.getChunkIndex();
    logger.info("Handling file chunk write for file {} at {}", request, chunkIndex);

    if (!getMetadataManager().existFileMeta(fileName) && !request.getCreateIfNotExists()) {
      logger.error("Cannot find file because it doesn't exist {}", fileName);

      replyBuilder.setStatus(
          MasterMetadataServiceOuterClass.OpenFileReply.OpenFileReplyStatusCode.NOT_FOUND);
      return false;
    }

    Optional<String> chunkHandleOptional =
        getMetadataManager().getChunkHandle(fileName, chunkIndex);
    if (chunkHandleOptional.isEmpty()) {
      if (!request.getCreateIfNotExists()) {
        logger.error("Cannot get chunk handle from file {} at {}", fileName, chunkIndex);

        replyBuilder.setStatus(
            MasterMetadataServiceOuterClass.OpenFileReply.OpenFileReplyStatusCode.INVALID_ARGUMENT);
        return false;
      }

      // Create file chunk handle
      logger.info("Creating a file chunk for {} at index {}", fileName, chunkIndex);
      boolean fileChunkCreationStatus = handleFileChunkCreation(request, replyBuilder);
      if (!fileChunkCreationStatus) {
        replyBuilder.setStatus(
            MasterMetadataServiceOuterClass.OpenFileReply.OpenFileReplyStatusCode.FAILED);
        return false;
      }

      chunkHandleOptional = getMetadataManager().getChunkHandle(fileName, chunkIndex);
      if (chunkHandleOptional.isEmpty()) {
        logger.error("Trying to create file chunk failed");

        replyBuilder.setStatus(
            MasterMetadataServiceOuterClass.OpenFileReply.OpenFileReplyStatusCode.FAILED);
        return false;
      }
    }

    Optional<Metadata.FileChunkMetadata> fileChunkMetadataOptional =
        getMetadataManager().getFileChunkMetadata(chunkHandleOptional.get());
    if (fileChunkMetadataOptional.isEmpty()) {
      logger.error("Cannot get file chunk metadata for chunk handle {}", chunkHandleOptional.get());

      replyBuilder.setStatus(
          MasterMetadataServiceOuterClass.OpenFileReply.OpenFileReplyStatusCode.FAILED);
      return false;
    }

    Metadata.FileChunkMetadata fileChunkMetadata = fileChunkMetadataOptional.get();
    final String chunkHandle = fileChunkMetadata.getChunkHandle();
    final int chunkVersion = fileChunkMetadata.getVersion();
    final int newChunkVersion = chunkVersion + 1;

    logger.info(
        "Advancing chunk version for chunk handle {} from {} to {}",
        chunkHandle,
        chunkVersion,
        newChunkVersion);

    List<ChunkServerOuterClass.ChunkServerLocation> advancedLocations = new LinkedList<>();
    for (ChunkServerOuterClass.ChunkServerLocation chunkServerLocation :
        getChunkServerManager().getChunkLocations(chunkHandle)) {
      // Get server address
      String serverHost = chunkServerLocation.getServerHostname();
      int serverPort = chunkServerLocation.getServerPort();
      String serverAddress = serverHost + ":" + serverPort;

      // Build request and send
      ChunkServerFileServiceClient client = getOrCreateChunkServerFileServiceClient(serverAddress);
      ChunkServerFileServiceOuterClass.AdvanceFileChunkVersionRequest.Builder builder =
          ChunkServerFileServiceOuterClass.AdvanceFileChunkVersionRequest.newBuilder();
      builder.setChunkHandle(chunkHandle);
      builder.setNewChunkVersion(newChunkVersion);

      ChunkServerFileServiceOuterClass.AdvanceFileChunkVersionReply reply =
          client.advanceFileChunkVersion(builder.build());
      if (reply.getStatus()
          != ChunkServerFileServiceOuterClass.AdvanceFileChunkVersionReply
              .AdvanceFileChunkVersionStatus.OK) {
        logger.error(
            "Failed to advance chunk version for chunk {} on server {}: {}",
            chunkHandle,
            serverAddress,
            reply);
        logger.warn("Skipping chunk server {}", serverAddress);
      } else {
        logger.info("Advanced chunk version for chunk {} on server {}", chunkHandle, serverAddress);
        advancedLocations.add(chunkServerLocation);
      }
    }

    if (advancedLocations.isEmpty()) {
      logger.error(
          "Unable to advance version on any of the chunk servers for handle: {}", chunkHandle);

      replyBuilder.setStatus(
          MasterMetadataServiceOuterClass.OpenFileReply.OpenFileReplyStatusCode.UNAVAILABLE);
      return false;
    }

    logger.info("Advanced chunk version for locations: {}", advancedLocations.toString());

    // Handle lease
    boolean leaseGranted = false;
    ChunkServerOuterClass.ChunkServerLocation primaryLocation = null;

    logger.info("Checking if we have lease for {}", chunkHandle);
    Optional<Map.Entry<ChunkServerOuterClass.ChunkServerLocation, Long>> primaryLeaseInfo =
        getMetadataManager().getPrimaryLeaseMetadata(chunkHandle);
    if (primaryLeaseInfo.isPresent()) {
      // Has lease
      ChunkServerOuterClass.ChunkServerLocation prevLeaseHolderLocation =
          primaryLeaseInfo.get().getKey();
      long leaseExpirationTime = primaryLeaseInfo.get().getValue();

      // Make sure old lease is still valid
      boolean leaseStillValid = false;
      for (ChunkServerOuterClass.ChunkServerLocation advanceLocation : advancedLocations) {
        if (advanceLocation.getServerHostname().equals(prevLeaseHolderLocation.getServerHostname())
            && advanceLocation.getServerPort() == prevLeaseHolderLocation.getServerPort()) {
          leaseStillValid = true;
        }
      }

      if (!leaseStillValid) {
        logger.error(
            "Old lease server for {} no longer available, will grant new lease", chunkHandle);
      } else {
        if (leaseExpirationTime > System.currentTimeMillis() / 1000) {
          logger.info(
              "Reuse existing lease for {}, held by {}",
              chunkHandle,
              prevLeaseHolderLocation.getServerHostname());

          leaseGranted = true;
          primaryLocation = prevLeaseHolderLocation;
        } else {
          logger.info("Original lease expired for {}", chunkHandle);
        }
      }
    }

    if (!leaseGranted) {
      for (ChunkServerOuterClass.ChunkServerLocation chunkServerLocation : advancedLocations) {
        String serverHost = chunkServerLocation.getServerHostname();
        int serverPort = chunkServerLocation.getServerPort();
        String serverAddress = serverHost + ":" + serverPort;

        logger.info(
            "Trying to grant write lease to server {} for chunk {}", serverAddress, chunkHandle);

        // Prepare GrantLeaseRequest and send
        ChunkServerLeaseServiceOuterClass.GrantLeaseRequest.Builder grantLeaseRequestBuilder =
            ChunkServerLeaseServiceOuterClass.GrantLeaseRequest.newBuilder();
        grantLeaseRequestBuilder.setChunkHandle(chunkHandle);
        grantLeaseRequestBuilder.setChunkVersion(chunkVersion + 1);
        long expirationUnixSec = System.currentTimeMillis() / 1000 + 60;
        grantLeaseRequestBuilder.setLeaseExpirationTime(
            Timestamp.newBuilder().setSeconds(expirationUnixSec).build());
        ChunkServerLeaseServiceOuterClass.GrantLeaseRequest grantLeaseRequest =
            grantLeaseRequestBuilder.build();

        ChunkServerLeaseServiceClient client =
            getOrCreateChunkServerLeaseServiceClient(serverAddress);
        ChunkServerLeaseServiceOuterClass.GrantLeaseReply grantLeaseReply =
            client.grantLease(grantLeaseRequest);
        if (grantLeaseReply.getStatus()
            != ChunkServerLeaseServiceOuterClass.GrantLeaseReply.GrantLeaseStatus.ACCEPTED) {
          logger.warn(
              "Failed to grant lease for chunk handle {} to chunk server {}: {}",
              chunkHandle,
              serverAddress,
              grantLeaseReply.toString());
        } else {
          // Accepted
          logger.info(
              "Grant lease request for chunk handle {} to chunk server {} succeeded",
              chunkHandle,
              serverAddress);

          leaseGranted = true;
          primaryLocation = chunkServerLocation;

          // Set metadata and break
          getMetadataManager()
              .setPrimaryLeaseMetadata(chunkHandle, primaryLocation, expirationUnixSec);
          break;
        }
      }
    }

    if (!leaseGranted) {
      logger.error("Failed to get lease after all attempts");

      replyBuilder.setStatus(
          MasterMetadataServiceOuterClass.OpenFileReply.OpenFileReplyStatusCode.UNAVAILABLE);
      return false;
    }

    // Setup reply
    Metadata.FileChunkMetadata.Builder fileChunkMetadataBuilder =
        Metadata.FileChunkMetadata.newBuilder();
    fileChunkMetadataBuilder.setChunkHandle(chunkHandle);
    fileChunkMetadataBuilder.setVersion(newChunkVersion);
    fileChunkMetadataBuilder.setPrimaryLocation(primaryLocation);
    for (ChunkServerOuterClass.ChunkServerLocation chunkServerLocation : advancedLocations) {
      fileChunkMetadataBuilder.addLocations(chunkServerLocation);
    }
    replyBuilder.setStatus(
        MasterMetadataServiceOuterClass.OpenFileReply.OpenFileReplyStatusCode.OK);
    replyBuilder.setMetadata(fileChunkMetadataBuilder.build());

    return true;
  }

  @Override
  public void openFile(
      org.shenzhu.grpcj.protos.MasterMetadataServiceOuterClass.OpenFileRequest request,
      io.grpc.stub.StreamObserver<
              org.shenzhu.grpcj.protos.MasterMetadataServiceOuterClass.OpenFileReply>
          responseObserver) {
    MasterMetadataServiceOuterClass.OpenFileReply.Builder replyBuilder =
        MasterMetadataServiceOuterClass.OpenFileReply.newBuilder();

    switch (request.getMode()) {
      case CREATE:
        handleFileCreation(request, replyBuilder);
        break;
      case READ:
        handleFileChunkRead(request, replyBuilder);
        break;
      case WRITE:
        handleFileChunkWrite(request, replyBuilder);
        break;
      default:
        replyBuilder.setStatus(
            MasterMetadataServiceOuterClass.OpenFileReply.OpenFileReplyStatusCode.INVALID_ARGUMENT);
    }

    responseObserver.onNext(replyBuilder.build());
    responseObserver.onCompleted();
  }

  @Override
  public void deleteFile(
      org.shenzhu.grpcj.protos.MasterMetadataServiceOuterClass.DeleteFileRequest request,
      io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
    final String fileName = request.getFilename();
    logger.info("Trying to delete file and chunk metadata associated with {}", fileName);

    getMetadataManager().deleteFileAndChunkMetadata(fileName);

    responseObserver.onCompleted();
  }
}
