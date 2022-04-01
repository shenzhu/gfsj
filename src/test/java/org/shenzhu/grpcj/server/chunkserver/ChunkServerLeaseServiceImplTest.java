package org.shenzhu.grpcj.server.chunkserver;

import com.google.protobuf.Timestamp;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.shenzhu.grpcj.protos.ChunkServerLeaseServiceGrpc;
import org.shenzhu.grpcj.protos.ChunkServerLeaseServiceOuterClass;

import static org.junit.Assert.assertSame;

public class ChunkServerLeaseServiceImplTest {
  @Rule public final GrpcCleanupRule grpcCleanupRule = new GrpcCleanupRule();

  private final String testGrantLeaseChunkHandle = "9d2a2342-97f9-11ea";
  private final String testRevokeLeaseChunkHandlePersist = "bb37-0242ac130002";
  private final String testRevokeLeaseChunkHandleRevokable = "0fd8e43c-a2e5-11ea";
  private final int testFileVersion = 2;
  private final long testExpirationUnixSeconds = System.currentTimeMillis() / 1000 + 3600;

  private ChunkServerLeaseServiceGrpc.ChunkServerLeaseServiceBlockingStub blockingStub;

  private ChunkServerLeaseServiceOuterClass.GrantLeaseRequest.Builder
      makeValidGrantLeaseRequestBuilder() {
    return ChunkServerLeaseServiceOuterClass.GrantLeaseRequest.newBuilder()
        .setChunkHandle(testGrantLeaseChunkHandle)
        .setChunkVersion(testFileVersion)
        .setLeaseExpirationTime(
            Timestamp.newBuilder().setSeconds(testExpirationUnixSeconds).build());
  }

  private ChunkServerLeaseServiceOuterClass.RevokeLeaseRequest.Builder
      makeValidRevokeLeaseRequestBuilder() {
    return ChunkServerLeaseServiceOuterClass.RevokeLeaseRequest.newBuilder()
        .setChunkHandle(testRevokeLeaseChunkHandlePersist)
        .setOriginalLeaseExpirationTime(
            Timestamp.newBuilder().setSeconds(testExpirationUnixSeconds).build());
  }

  @Before
  public void SetUp() throws Exception {
    // Initialize file chunk manager
    FileChunkManager fileChunkManager = FileChunkManager.getInstance();
    fileChunkManager.initialize("ChunkServerLeaseServiceImplTestDB", 1024);
    fileChunkManager.createChunk(testGrantLeaseChunkHandle, testFileVersion);
    fileChunkManager.createChunk(testRevokeLeaseChunkHandlePersist, testFileVersion);
    fileChunkManager.createChunk(testRevokeLeaseChunkHandleRevokable, testFileVersion);

    ChunkServerImpl chunkServer = new ChunkServerImpl(fileChunkManager);
    chunkServer.addOrUpdateLease(testRevokeLeaseChunkHandlePersist, testExpirationUnixSeconds);
    chunkServer.addOrUpdateLease(testRevokeLeaseChunkHandleRevokable, testExpirationUnixSeconds);

    // Generate a unique in-process server name
    String serverName = InProcessServerBuilder.generateName();

    // Create a server, add service, start, and register for automatic graceful shutdown
    grpcCleanupRule.register(
        InProcessServerBuilder.forName(serverName)
            .directExecutor()
            .addService(new ChunkServerLeaseServiceImpl(chunkServer))
            .build()
            .start());

    blockingStub =
        ChunkServerLeaseServiceGrpc.newBlockingStub(
            // Create a server, add service, start, and register for automatic graceful shutdown
            grpcCleanupRule.register(
                InProcessChannelBuilder.forName(serverName).directExecutor().build()));
  }

  @Test
  public void testGrantLeaseValidRequest() {
    ChunkServerLeaseServiceOuterClass.GrantLeaseRequest request =
        makeValidGrantLeaseRequestBuilder().build();

    ChunkServerLeaseServiceOuterClass.GrantLeaseReply reply = blockingStub.grantLease(request);
    assertSame(
        reply.getStatus(),
        ChunkServerLeaseServiceOuterClass.GrantLeaseReply.GrantLeaseStatus.ACCEPTED);
  }

  @Test
  public void testGrantLeaseNoChunkHandle() {
    ChunkServerLeaseServiceOuterClass.GrantLeaseRequest request =
        makeValidGrantLeaseRequestBuilder().setChunkHandle("no-handle").build();

    ChunkServerLeaseServiceOuterClass.GrantLeaseReply reply = blockingStub.grantLease(request);
    assertSame(
        reply.getStatus(),
        ChunkServerLeaseServiceOuterClass.GrantLeaseReply.GrantLeaseStatus.REJECTED_NOT_FOUND);
  }

  @Test
  public void testGrantLeaseStaleVersion() {
    ChunkServerLeaseServiceOuterClass.GrantLeaseRequest request =
        makeValidGrantLeaseRequestBuilder().setChunkVersion(testFileVersion + 1).build();

    ChunkServerLeaseServiceOuterClass.GrantLeaseReply reply = blockingStub.grantLease(request);
    assertSame(
        reply.getStatus(),
        ChunkServerLeaseServiceOuterClass.GrantLeaseReply.GrantLeaseStatus.REJECTED_STALE_VERSION);
  }

  @Test
  public void testGrantLeaseStaleVersionInRequest() {
    ChunkServerLeaseServiceOuterClass.GrantLeaseRequest request =
        makeValidGrantLeaseRequestBuilder().setChunkVersion(testFileVersion - 1).build();

    ChunkServerLeaseServiceOuterClass.GrantLeaseReply reply = blockingStub.grantLease(request);
    assertSame(
        reply.getStatus(),
        ChunkServerLeaseServiceOuterClass.GrantLeaseReply.GrantLeaseStatus.UNKNOWN);
  }

  @Test
  public void testGrantLeaseExpiredLease() {
    ChunkServerLeaseServiceOuterClass.GrantLeaseRequest request =
        makeValidGrantLeaseRequestBuilder()
            .setLeaseExpirationTime(
                Timestamp.newBuilder().setSeconds(System.currentTimeMillis() / 1000 - 60).build())
            .build();

    ChunkServerLeaseServiceOuterClass.GrantLeaseReply reply = blockingStub.grantLease(request);
    assertSame(
        reply.getStatus(),
        ChunkServerLeaseServiceOuterClass.GrantLeaseReply.GrantLeaseStatus.IGNORED_EXPIRED_LEASE);
  }

  @Test
  public void testRevokeLeaseValidRequest() {
    ChunkServerLeaseServiceOuterClass.RevokeLeaseRequest request =
        makeValidRevokeLeaseRequestBuilder()
            .setChunkHandle(testRevokeLeaseChunkHandleRevokable)
            .build();

    ChunkServerLeaseServiceOuterClass.RevokeLeaseReply reply = blockingStub.revokeLease(request);
    assertSame(
        reply.getStatus(),
        ChunkServerLeaseServiceOuterClass.RevokeLeaseReply.RevokeLeaseStatus.REVOKED);
  }

  @Test
  public void testLeaseNewerLeaseAlreadyExists() {
    ChunkServerLeaseServiceOuterClass.RevokeLeaseRequest request =
        makeValidRevokeLeaseRequestBuilder()
            .setOriginalLeaseExpirationTime(
                Timestamp.newBuilder().setSeconds(testExpirationUnixSeconds - 1000))
            .build();

    ChunkServerLeaseServiceOuterClass.RevokeLeaseReply reply = blockingStub.revokeLease(request);
    assertSame(
        reply.getStatus(),
        ChunkServerLeaseServiceOuterClass.RevokeLeaseReply.RevokeLeaseStatus
            .IGNORED_HAS_NEWER_LEASE);
  }

  @Test
  public void testRevokeLeaseNoChunkHandle() {
    ChunkServerLeaseServiceOuterClass.RevokeLeaseRequest request =
        makeValidRevokeLeaseRequestBuilder().setChunkHandle("no-handle").build();

    ChunkServerLeaseServiceOuterClass.RevokeLeaseReply reply = blockingStub.revokeLease(request);
    assertSame(
        reply.getStatus(),
        ChunkServerLeaseServiceOuterClass.RevokeLeaseReply.RevokeLeaseStatus.REJECTED_NOT_FOUND);
  }
}
