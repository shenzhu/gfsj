package org.shenzhu.grpcj.client;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.shenzhu.grpcj.protos.ChunkServerFileServiceOuterClass;
import org.shenzhu.grpcj.protos.ChunkServerOuterClass;
import org.shenzhu.grpcj.protos.MasterMetadataServiceOuterClass;
import org.shenzhu.grpcj.utils.Checksum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;

public class GFSClient {
  private static Logger logger = LoggerFactory.getLogger(GFSClient.class);

  public static MasterMetadataServiceClient getMasterMetadataServiceClient() {
    // Establishing connection to master
    ManagedChannel channel =
        ManagedChannelBuilder.forTarget("localhost:50050").usePlaintext().build();
    return new MasterMetadataServiceClient(channel);
  }

  public static ChunkServerFileServiceClient getChunkServerFileServiceClient() {
    // Establishing connection to chunk server
    ManagedChannel channel =
        ManagedChannelBuilder.forTarget("localhost:50051").usePlaintext().build();
    return new ChunkServerFileServiceClient(channel);
  }

  public static void createFile(String fileName) {
    MasterMetadataServiceOuterClass.OpenFileRequest.Builder builder =
        MasterMetadataServiceOuterClass.OpenFileRequest.newBuilder();
    builder.setFilename(fileName);
    builder.setMode(MasterMetadataServiceOuterClass.OpenFileRequest.OpenMode.CREATE);

    MasterMetadataServiceClient client = getMasterMetadataServiceClient();
    MasterMetadataServiceOuterClass.OpenFileReply reply = client.openFile(builder.build());
  }

  public static void writeData(String fileName, String data) throws NoSuchAlgorithmException {
    int chunkIndex = 0;

    // Establishing connection to master
    MasterMetadataServiceClient client = getMasterMetadataServiceClient();

    // Get metadata for current file chunk
    MasterMetadataServiceOuterClass.OpenFileRequest.Builder openFileRequestBuilder =
        MasterMetadataServiceOuterClass.OpenFileRequest.newBuilder();
    openFileRequestBuilder.setFilename(fileName);
    openFileRequestBuilder.setChunkIndex(chunkIndex);
    openFileRequestBuilder.setMode(MasterMetadataServiceOuterClass.OpenFileRequest.OpenMode.WRITE);
    openFileRequestBuilder.setCreateIfNotExists(true);
    MasterMetadataServiceOuterClass.OpenFileRequest openFileRequest =
        openFileRequestBuilder.build();
    logger.info(
        "Sending OpenFileRequest to file {} at chunk index {} with mode {}",
        fileName,
        chunkIndex,
        MasterMetadataServiceOuterClass.OpenFileRequest.OpenMode.WRITE);

    MasterMetadataServiceOuterClass.OpenFileReply reply = client.openFile(openFileRequest);
    logger.info(
        "Received metadata for chunk at {} in {}: {}", chunkIndex, fileName, reply.getMetadata());

    // Send data to write to ChunkServers
    for (ChunkServerOuterClass.ChunkServerLocation chunkServerLocation :
        reply.getMetadata().getLocationsList()) {
      ChunkServerFileServiceOuterClass.SendChunkDataRequest.Builder sendChunkDataRequestBuilder =
          ChunkServerFileServiceOuterClass.SendChunkDataRequest.newBuilder();
      sendChunkDataRequestBuilder.setData(ByteString.copyFrom(data, StandardCharsets.UTF_8));
      sendChunkDataRequestBuilder.setChecksum(
          ByteString.copyFrom(Checksum.getCheckSum(data.getBytes(StandardCharsets.UTF_8))));

      ChunkServerFileServiceClient chunkServerFileServiceClient = getChunkServerFileServiceClient();
      ChunkServerFileServiceOuterClass.SendChunkDataReply sendChunkDataReply =
          chunkServerFileServiceClient.sendChunkData(sendChunkDataRequestBuilder.build());
    }

    // Issue WriteFileChunkRequest to primary chunk server
    ChunkServerFileServiceOuterClass.WriteFileChunkRequest.Builder writeFileChunkRequestBuilder =
        ChunkServerFileServiceOuterClass.WriteFileChunkRequest.newBuilder();
    writeFileChunkRequestBuilder
        .setHeader(
            ChunkServerFileServiceOuterClass.WriteFileChunkRequestHeader.newBuilder()
                .setChunkHandle(reply.getMetadata().getChunkHandle())
                .setChunkVersion(reply.getMetadata().getVersion())
                .setOffsetStart(0)
                .setLength(data.getBytes(StandardCharsets.UTF_8).length)
                .setDataChecksum(
                    ByteString.copyFrom(
                        Checksum.getCheckSum(data.getBytes(StandardCharsets.UTF_8))))
                .build())
        .addAllReplicaLocations(reply.getMetadata().getLocationsList());
    ChunkServerFileServiceClient chunkServerFileServiceClient = getChunkServerFileServiceClient();
    ChunkServerFileServiceOuterClass.WriteFileChunkReply writeFileChunkReply =
        chunkServerFileServiceClient.writeFileChunk(writeFileChunkRequestBuilder.build());
  }

  public static void readData(String fileName, int chunkIndex, int offset, int bytes) {
    // Establishing connection to master
    MasterMetadataServiceClient client = getMasterMetadataServiceClient();

    // Get metadata for current file chunk
    MasterMetadataServiceOuterClass.OpenFileRequest.Builder openFileRequestBuilder =
        MasterMetadataServiceOuterClass.OpenFileRequest.newBuilder();
    openFileRequestBuilder.setFilename(fileName);
    openFileRequestBuilder.setChunkIndex(chunkIndex);
    openFileRequestBuilder.setMode(MasterMetadataServiceOuterClass.OpenFileRequest.OpenMode.READ);
    openFileRequestBuilder.setCreateIfNotExists(true);
    MasterMetadataServiceOuterClass.OpenFileRequest openFileRequest =
        openFileRequestBuilder.build();
    logger.info(
        "Sending OpenFileRequest to file {} at chunk index {} with mode {}",
        fileName,
        chunkIndex,
        MasterMetadataServiceOuterClass.OpenFileRequest.OpenMode.READ);

    MasterMetadataServiceOuterClass.OpenFileReply reply = client.openFile(openFileRequest);
    logger.info(
        "Received metadata for chunk at {} in {}: {}", chunkIndex, fileName, reply.getMetadata());

    for (ChunkServerOuterClass.ChunkServerLocation chunkServerLocation :
        reply.getMetadata().getLocationsList()) {
      ChunkServerFileServiceOuterClass.ReadFileChunkRequest.Builder readFileChunkRequestBuilder =
          ChunkServerFileServiceOuterClass.ReadFileChunkRequest.newBuilder();
      readFileChunkRequestBuilder.setChunkHandle(reply.getMetadata().getChunkHandle());
      readFileChunkRequestBuilder.setChunkVersion(reply.getMetadata().getVersion());
      readFileChunkRequestBuilder.setOffsetStart(offset);
      readFileChunkRequestBuilder.setLength(bytes);

      ChunkServerFileServiceClient chunkServerFileServiceClient = getChunkServerFileServiceClient();
      ChunkServerFileServiceOuterClass.ReadFileChunkReply readFileChunkReply =
          chunkServerFileServiceClient.readFileChunk(readFileChunkRequestBuilder.build());
      if (readFileChunkReply.getStatus()
          == ChunkServerFileServiceOuterClass.ReadFileChunkReply.ReadFileChunkStatus.OK) {
        String data = readFileChunkReply.getData().toString(StandardCharsets.UTF_8);
        logger.info("Read data '{}' from chunk server: {}", data, chunkServerLocation);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    String fileName = "testFile1";
    String data = "hello world";

    createFile(fileName);
    writeData(fileName, data);
    readData(fileName, 0, 0, 11);
  }
}
