package org.shenzhu.grpcj.server.masterserver;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.shenzhu.grpcj.protos.ChunkServerOuterClass;
import org.shenzhu.grpcj.protos.MasterMetadataServiceGrpc;
import org.shenzhu.grpcj.protos.MasterMetadataServiceOuterClass;
import org.shenzhu.grpcj.server.chunkserver.ChunkServerControlServiceImpl;
import org.shenzhu.grpcj.server.chunkserver.ChunkServerFileServiceImpl;
import org.shenzhu.grpcj.server.chunkserver.ChunkServerImpl;
import org.shenzhu.grpcj.server.chunkserver.ChunkServerLeaseServiceImpl;
import org.shenzhu.grpcj.server.chunkserver.FileChunkManager;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

// TODO: Somehow running 3 tests together testHandleFileChunkWrite will fail, but running
// individually all 3 could pass
public class MasterMetadataServiceImplTest {
  @Rule public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  private final String masterMetadataService = InProcessServerBuilder.generateName();
  private MasterMetadataServiceGrpc.MasterMetadataServiceBlockingStub blockingStub;

  private Thread chunkServerThread;

  @Before
  public void SetUp() throws IOException, InterruptedException {
    // Set up chunk server related services
    chunkServerThread =
        new Thread() {
          @Override
          public void run() {
            // Initialize FileChunkManager
            FileChunkManager.getInstance().initialize("MasterMetadataServiceImplTestDB", 1000);
            FileChunkManager.getInstance().deleteChunk("0");

            // Create dummy channel for chunk server impl
            ManagedChannel channel =
                ManagedChannelBuilder.forTarget("localhost:8088").usePlaintext().build();
            ChunkServerImpl chunkServerImpl =
                new ChunkServerImpl(FileChunkManager.getInstance(), channel);

            // Create and start chunk server related services
            int port = 50051;
            try {
              Server server =
                  ServerBuilder.forPort(port)
                      .addService(
                          new ChunkServerFileServiceImpl(
                              FileChunkManager.getInstance(), chunkServerImpl))
                      .addService(new ChunkServerLeaseServiceImpl(chunkServerImpl))
                      .addService(new ChunkServerControlServiceImpl(chunkServerImpl))
                      .build()
                      .start();

              server.awaitTermination();
            } catch (IOException | InterruptedException ignored) {
            }
          }
        };
    chunkServerThread.start();

    // Set up master server metadata service
    grpcCleanup.register(
        InProcessServerBuilder.forName(masterMetadataService)
            .directExecutor()
            .addService(new MasterMetadataServiceImpl())
            .build()
            .start());
    blockingStub =
        MasterMetadataServiceGrpc.newBlockingStub(
            grpcCleanup.register(
                InProcessChannelBuilder.forName(masterMetadataService).directExecutor().build()));

    // Register chunk server
    ChunkServerOuterClass.ChunkServerLocation chunkServerLocation =
        ChunkServerOuterClass.ChunkServerLocation.newBuilder()
            .setServerHostname("localhost")
            .setServerPort(50051)
            .build();
    ChunkServerManager.getInstance()
        .registerChunkServer(
            ChunkServerOuterClass.ChunkServer.newBuilder()
                .setLocation(chunkServerLocation)
                .setAvailableDiskMb(1000)
                .build());
  }

  @Test
  public void testHandleFileCreation() {
    MasterMetadataServiceOuterClass.OpenFileRequest.Builder builder =
        MasterMetadataServiceOuterClass.OpenFileRequest.newBuilder();
    builder.setFilename("testHandleFileCreation");
    builder.setChunkIndex(0);
    builder.setMode(MasterMetadataServiceOuterClass.OpenFileRequest.OpenMode.CREATE);
    builder.setCreateIfNotExists(true);

    MasterMetadataServiceOuterClass.OpenFileReply reply =
        this.blockingStub.openFile(builder.build());

    assertEquals(reply.getMetadata().getVersion(), 1);
    assertTrue("012".contains(reply.getMetadata().getChunkHandle()));
    assertEquals(reply.getMetadata().getPrimaryLocation().getServerHostname(), "localhost");
    assertEquals(reply.getMetadata().getPrimaryLocation().getServerPort(), 50051);
    assertSame(
        reply.getStatus(),
        MasterMetadataServiceOuterClass.OpenFileReply.OpenFileReplyStatusCode.OK);
  }

  @Test
  public void testHandleFileChunkWrite() {
    // Create file first
    MasterMetadataServiceOuterClass.OpenFileRequest.Builder builder =
        MasterMetadataServiceOuterClass.OpenFileRequest.newBuilder();
    builder.setFilename("testHandleFileChunkWrite");
    builder.setChunkIndex(0);
    builder.setMode(MasterMetadataServiceOuterClass.OpenFileRequest.OpenMode.CREATE);

    MasterMetadataServiceOuterClass.OpenFileReply reply =
        this.blockingStub.openFile(builder.build());
    assertSame(
        reply.getStatus(),
        MasterMetadataServiceOuterClass.OpenFileReply.OpenFileReplyStatusCode.OK);

    // Prepare data for master server
    ChunkServerManager.getInstance()
        .updateChunkServer(
            ChunkServerOuterClass.ChunkServerLocation.newBuilder()
                .setServerHostname("localhost")
                .setServerPort(50051)
                .build(),
            1000,
            new HashSet<String>(Collections.singleton("0")),
            new HashSet<String>());

    // File chunk writer
    builder = MasterMetadataServiceOuterClass.OpenFileRequest.newBuilder();
    builder.setFilename("testHandleFileChunkWrite");
    builder.setChunkIndex(0);
    builder.setMode(MasterMetadataServiceOuterClass.OpenFileRequest.OpenMode.WRITE);
    builder.setCreateIfNotExists(true);

    reply = this.blockingStub.openFile(builder.build());
    assertSame(
        reply.getStatus(),
        MasterMetadataServiceOuterClass.OpenFileReply.OpenFileReplyStatusCode.OK);
    assertTrue("012".contains(reply.getMetadata().getChunkHandle()));
    assertEquals(reply.getMetadata().getVersion(), 2);
    assertEquals(reply.getMetadata().getPrimaryLocation().getServerHostname(), "localhost");
    assertEquals(reply.getMetadata().getPrimaryLocation().getServerPort(), 50051);
    assertEquals(reply.getMetadata().getLocationsCount(), 1);
    assertEquals(reply.getMetadata().getLocations(0).getServerHostname(), "localhost");
    assertEquals(reply.getMetadata().getLocations(0).getServerPort(), 50051);
  }

  @Test
  public void testHandleFileChunkRead() {
    // Create file first
    MasterMetadataServiceOuterClass.OpenFileRequest.Builder builder =
        MasterMetadataServiceOuterClass.OpenFileRequest.newBuilder();
    builder.setFilename("testHandleFileChunkRead");
    builder.setChunkIndex(0);
    builder.setMode(MasterMetadataServiceOuterClass.OpenFileRequest.OpenMode.CREATE);

    MasterMetadataServiceOuterClass.OpenFileReply reply =
        this.blockingStub.openFile(builder.build());
    assertSame(
        reply.getStatus(),
        MasterMetadataServiceOuterClass.OpenFileReply.OpenFileReplyStatusCode.OK);

    // File chunk reader
    builder = MasterMetadataServiceOuterClass.OpenFileRequest.newBuilder();
    builder.setFilename("testHandleFileChunkRead");
    builder.setChunkIndex(0);
    builder.setMode(MasterMetadataServiceOuterClass.OpenFileRequest.OpenMode.READ);

    reply = this.blockingStub.openFile(builder.build());
    assertSame(
        reply.getStatus(),
        MasterMetadataServiceOuterClass.OpenFileReply.OpenFileReplyStatusCode.OK);
    assertTrue("012".contains(reply.getMetadata().getChunkHandle()));
    assertEquals(reply.getMetadata().getVersion(), 1);
    assertEquals(reply.getMetadata().getLocationsCount(), 1);
    assertEquals(reply.getMetadata().getLocations(0).getServerHostname(), "localhost");
    assertEquals(reply.getMetadata().getLocations(0).getServerPort(), 50051);
  }

  @After
  public void TearDown() {
    chunkServerThread.interrupt();
  }
}
