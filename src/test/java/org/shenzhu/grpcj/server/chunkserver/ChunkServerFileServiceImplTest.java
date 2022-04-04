package org.shenzhu.grpcj.server.chunkserver;

import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.shenzhu.grpcj.protos.ChunkServerFileServiceGrpc;
import org.shenzhu.grpcj.protos.ChunkServerFileServiceOuterClass;
import org.shenzhu.grpcj.protos.ChunkServerLeaseServiceGrpc;
import org.shenzhu.grpcj.protos.ChunkServerLeaseServiceOuterClass;
import org.shenzhu.grpcj.utils.Checksum;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class ChunkServerFileServiceImplTest {
  @Rule public final GrpcCleanupRule grpcCleanupRule = new GrpcCleanupRule();

  // Test data
  private final String kTestFileHandle = "9d2a2342-97f9-11ea";
  private final String kTestFileHandleAdvanceable = "bb37-0242ac130002";
  private final int kTestFileVersion = 2;
  private final String kTestData = "Hello, World! This is a dope test";
  private final long kTestLeaseExpirationUnixSeconds = System.currentTimeMillis() / 1000 + 3600;

  private ChunkServerFileServiceGrpc.ChunkServerFileServiceBlockingStub blockingStub;
  private ChunkServerLeaseServiceGrpc.ChunkServerLeaseServiceBlockingStub leaseServiceBlockingStub;

  private ChunkServerFileServiceOuterClass.ReadFileChunkRequest.Builder
      makeValidReadFileChunkRequestBuilder() {
    ChunkServerFileServiceOuterClass.ReadFileChunkRequest.Builder builder =
        ChunkServerFileServiceOuterClass.ReadFileChunkRequest.newBuilder();

    builder.setChunkHandle(kTestFileHandle);
    builder.setChunkVersion(kTestFileVersion);
    builder.setOffsetStart(0);
    builder.setLength(kTestData.length());

    return builder;
  }

  private ChunkServerFileServiceOuterClass.AdvanceFileChunkVersionRequest.Builder
      makeValidAdvanceFileChunkVersionRequestBuilder() {
    ChunkServerFileServiceOuterClass.AdvanceFileChunkVersionRequest.Builder builder =
        ChunkServerFileServiceOuterClass.AdvanceFileChunkVersionRequest.newBuilder();

    builder.setChunkHandle(kTestFileHandle);
    builder.setNewChunkVersion(kTestFileVersion + 1);

    return builder;
  }

  private void seedTestData() {
    FileChunkManager fileChunkManager = FileChunkManager.getInstance();

    // clean data first
    fileChunkManager.deleteChunk(kTestFileHandle);
    fileChunkManager.deleteChunk(kTestFileHandleAdvanceable);

    fileChunkManager.createChunk(kTestFileHandle, kTestFileVersion);
    fileChunkManager.writeToChunk(
        kTestFileHandle, kTestFileVersion, 0, kTestData.length(), kTestData);

    fileChunkManager.createChunk(kTestFileHandleAdvanceable, kTestFileVersion);
    fileChunkManager.writeToChunk(
        kTestFileHandleAdvanceable, kTestFileVersion, 0, kTestData.length(), kTestData);
  }

  @Before
  public void SetUp() throws Exception {
    // Generate a unique in-process server name
    String serverName = InProcessServerBuilder.generateName();
    String masterServerName = InProcessServerBuilder.generateName();

    FileChunkManager fileChunkManager = FileChunkManager.getInstance();
    fileChunkManager.initialize("UnitTestDB", 1000);

    ChunkServerImpl chunkServerImpl =
        new ChunkServerImpl(
            fileChunkManager,
            grpcCleanupRule.register(
                InProcessChannelBuilder.forName(masterServerName).directExecutor().build()));

    // Create a server, add service, start, and register for automatic graceful shutdown
    grpcCleanupRule.register(
        InProcessServerBuilder.forName(serverName)
            .directExecutor()
            .addService(new ChunkServerFileServiceImpl(fileChunkManager, chunkServerImpl))
            .addService(new ChunkServerLeaseServiceImpl(chunkServerImpl))
            .build()
            .start());

    blockingStub =
        ChunkServerFileServiceGrpc.newBlockingStub(
            // Create a server, add service, start, and register for automatic graceful shutdown
            grpcCleanupRule.register(
                InProcessChannelBuilder.forName(serverName).directExecutor().build()));

    leaseServiceBlockingStub =
        ChunkServerLeaseServiceGrpc.newBlockingStub(
            grpcCleanupRule.register(
                InProcessChannelBuilder.forName(serverName).directExecutor().build()));

    seedTestData();
  }

  @Test
  public void testInitFileChunkOK() throws Exception {
    String chunkHandle = UUID.randomUUID().toString();

    ChunkServerFileServiceOuterClass.InitFileChunkRequest request =
        ChunkServerFileServiceOuterClass.InitFileChunkRequest.newBuilder()
            .setChunkHandle(chunkHandle)
            .build();

    ChunkServerFileServiceOuterClass.InitFileChunkReply reply = blockingStub.initFileChunk(request);

    assertEquals(
        reply.getStatus(),
        ChunkServerFileServiceOuterClass.InitFileChunkReply.InitFileChunkStatus.CREATED);
    assertEquals(reply.getRequest().getChunkHandle(), chunkHandle);
  }

  @Test
  public void testReadFileChunkOK() {
    ChunkServerFileServiceOuterClass.ReadFileChunkRequest request =
        makeValidReadFileChunkRequestBuilder().build();

    ChunkServerFileServiceOuterClass.ReadFileChunkReply reply = blockingStub.readFileChunk(request);

    assertSame(
        reply.getStatus(),
        ChunkServerFileServiceOuterClass.ReadFileChunkReply.ReadFileChunkStatus.OK);
    assertEquals(reply.getData().toString(StandardCharsets.UTF_8), kTestData);
    assertEquals(reply.getBytesRead(), kTestData.length());
  }

  @Test
  public void testReadFileChunkFullRead() {
    ChunkServerFileServiceOuterClass.ReadFileChunkRequest.Builder builder =
        makeValidReadFileChunkRequestBuilder();
    final int readLength = kTestData.length() - 3;
    builder.setLength(readLength);

    ChunkServerFileServiceOuterClass.ReadFileChunkReply reply =
        blockingStub.readFileChunk(builder.build());

    assertSame(
        reply.getStatus(),
        ChunkServerFileServiceOuterClass.ReadFileChunkReply.ReadFileChunkStatus.OK);
    assertEquals(
        reply.getData().toString(StandardCharsets.UTF_8), kTestData.substring(0, readLength));
    assertEquals(reply.getBytesRead(), readLength);
  }

  @Test
  public void testReadFileChunkNoChunk() {
    ChunkServerFileServiceOuterClass.ReadFileChunkRequest.Builder builder =
        makeValidReadFileChunkRequestBuilder();
    builder.setChunkHandle("no-handle");

    ChunkServerFileServiceOuterClass.ReadFileChunkReply reply =
        blockingStub.readFileChunk(builder.build());
    assertSame(
        reply.getStatus(),
        ChunkServerFileServiceOuterClass.ReadFileChunkReply.ReadFileChunkStatus.UNKNOWN);
  }

  @Test
  public void testAdvanceFileChunkVersionOK() {
    ChunkServerFileServiceOuterClass.AdvanceFileChunkVersionRequest.Builder builder =
        makeValidAdvanceFileChunkVersionRequestBuilder();

    builder.setChunkHandle(kTestFileHandleAdvanceable);

    ChunkServerFileServiceOuterClass.AdvanceFileChunkVersionReply reply =
        blockingStub.advanceFileChunkVersion(builder.build());
    assertSame(
        reply.getStatus(),
        ChunkServerFileServiceOuterClass.AdvanceFileChunkVersionReply.AdvanceFileChunkVersionStatus
            .OK);
  }

  @Test
  public void testAdvanceFileChunkVersionNoFileChunk() {
    ChunkServerFileServiceOuterClass.AdvanceFileChunkVersionRequest.Builder builder =
        makeValidAdvanceFileChunkVersionRequestBuilder();
    builder.setChunkHandle("no-handle");

    ChunkServerFileServiceOuterClass.AdvanceFileChunkVersionReply reply =
        blockingStub.advanceFileChunkVersion(builder.build());
    assertSame(
        reply.getStatus(),
        ChunkServerFileServiceOuterClass.AdvanceFileChunkVersionReply.AdvanceFileChunkVersionStatus
            .UNKNOWN);
  }

  @Test
  public void testWriteFileChunk() throws Exception {
    // Prepare data
    final String chunkHandle = "WriteReplicatedFileChunk";
    final String writeData = "Data for WriteReplicatedFileChunk";
    final byte[] writeDataChecksum =
        Checksum.getCheckSum(writeData.getBytes(StandardCharsets.UTF_8));

    // Delete data if exists
    FileChunkManager.getInstance().deleteChunk(chunkHandle);

    ChunkServerFileServiceOuterClass.WriteFileChunkRequestHeader.Builder headerBuilder =
        ChunkServerFileServiceOuterClass.WriteFileChunkRequestHeader.newBuilder();
    headerBuilder.setChunkHandle(chunkHandle);
    headerBuilder.setChunkVersion(kTestFileVersion);
    headerBuilder.setOffsetStart(0);
    headerBuilder.setLength(writeData.length());
    headerBuilder.setDataChecksum(ByteString.copyFrom(writeDataChecksum));

    ChunkServerFileServiceOuterClass.WriteFileChunkRequest.Builder requestBuilder =
        ChunkServerFileServiceOuterClass.WriteFileChunkRequest.newBuilder();
    requestBuilder.setHeader(headerBuilder.build());

    // First write, should fail since current chunk server is not leaseholder
    ChunkServerFileServiceOuterClass.WriteFileChunkReply reply =
        blockingStub.writeFileChunk(requestBuilder.build());
    assertSame(
        reply.getStatus(),
        ChunkServerFileServiceOuterClass.FileChunkMutationStatus.FAILED_NOT_LEASE_HOLDER);

    // Init chunk handle on chunk server
    ChunkServerFileServiceOuterClass.InitFileChunkRequest.Builder initFileChunkRequestBuilder =
        ChunkServerFileServiceOuterClass.InitFileChunkRequest.newBuilder();
    initFileChunkRequestBuilder.setChunkHandle(chunkHandle);

    ChunkServerFileServiceOuterClass.InitFileChunkReply initFileChunkReply =
        blockingStub.initFileChunk(initFileChunkRequestBuilder.build());
    assertSame(
        initFileChunkReply.getStatus(),
        ChunkServerFileServiceOuterClass.InitFileChunkReply.InitFileChunkStatus.CREATED);

    // Advance version of chunk handle on chunk server
    ChunkServerFileServiceOuterClass.AdvanceFileChunkVersionRequest.Builder
        advanceFileChunkVersionBuilder =
            ChunkServerFileServiceOuterClass.AdvanceFileChunkVersionRequest.newBuilder();
    advanceFileChunkVersionBuilder.setChunkHandle(chunkHandle);
    advanceFileChunkVersionBuilder.setNewChunkVersion(kTestFileVersion);

    ChunkServerFileServiceOuterClass.AdvanceFileChunkVersionReply advanceFileChunkVersionReply =
        blockingStub.advanceFileChunkVersion(advanceFileChunkVersionBuilder.build());
    assertSame(
        advanceFileChunkVersionReply.getStatus(),
        ChunkServerFileServiceOuterClass.AdvanceFileChunkVersionReply.AdvanceFileChunkVersionStatus
            .OK);

    // Prepare lease
    ChunkServerLeaseServiceOuterClass.GrantLeaseRequest.Builder grantLeaseRequestBuilder =
        ChunkServerLeaseServiceOuterClass.GrantLeaseRequest.newBuilder();
    grantLeaseRequestBuilder.setChunkHandle(chunkHandle);
    grantLeaseRequestBuilder.setChunkVersion(kTestFileVersion);
    grantLeaseRequestBuilder.setLeaseExpirationTime(
        Timestamp.newBuilder().setSeconds(kTestLeaseExpirationUnixSeconds).build());

    // Grant lease
    ChunkServerLeaseServiceOuterClass.GrantLeaseReply grantLeaseReply =
        leaseServiceBlockingStub.grantLease(grantLeaseRequestBuilder.build());
    assertSame(
        grantLeaseReply.getStatus(),
        ChunkServerLeaseServiceOuterClass.GrantLeaseReply.GrantLeaseStatus.ACCEPTED);

    // Write file chunk again, should fail again since data has not already been sent to chunk
    // server
    reply = blockingStub.writeFileChunk(requestBuilder.build());
    assertSame(
        reply.getStatus(),
        ChunkServerFileServiceOuterClass.FileChunkMutationStatus.FAILED_DATA_NOT_FOUND);

    // Send data to chunk server
    ChunkServerFileServiceOuterClass.SendChunkDataRequest.Builder sendChunkDataRequestBuilder =
        ChunkServerFileServiceOuterClass.SendChunkDataRequest.newBuilder();
    sendChunkDataRequestBuilder.setData(ByteString.copyFrom(writeData, StandardCharsets.UTF_8));
    sendChunkDataRequestBuilder.setChecksum(ByteString.copyFrom(writeDataChecksum));

    ChunkServerFileServiceOuterClass.SendChunkDataReply sendChunkDataReply =
        blockingStub.sendChunkData(sendChunkDataRequestBuilder.build());
    assertSame(
        sendChunkDataReply.getStatus(),
        ChunkServerFileServiceOuterClass.SendChunkDataReply.SendChunkDataRequestStatus.OK);

    // Write file chunk again x 2
    reply = blockingStub.writeFileChunk(requestBuilder.build());
    assertSame(reply.getStatus(), ChunkServerFileServiceOuterClass.FileChunkMutationStatus.OK);
    assertEquals(reply.getBytesWritten(), kTestData.length());
  }
}
