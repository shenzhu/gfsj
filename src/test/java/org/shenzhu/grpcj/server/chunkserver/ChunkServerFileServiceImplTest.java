package org.shenzhu.grpcj.server.chunkserver;

import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.shenzhu.grpcj.protos.ChunkServerFileServiceGrpc;
import org.shenzhu.grpcj.protos.ChunkServerFileServiceOuterClass;

import java.util.UUID;

import static org.junit.Assert.assertEquals;

public class ChunkServerFileServiceImplTest {
  @Rule public final GrpcCleanupRule grpcCleanupRule = new GrpcCleanupRule();

  private ChunkServerFileServiceGrpc.ChunkServerFileServiceBlockingStub blockingStub;

  @Before
  public void SetUp() throws Exception {
    // Generate a unique in-process server name
    String serverName = InProcessServerBuilder.generateName();

    FileChunkManager fileChunkManager = FileChunkManager.getInstance();
    fileChunkManager.initialize("UnitTestDB", 1000);

    // Create a server, add service, start, and register for automatic graceful shutdown
    grpcCleanupRule.register(
        InProcessServerBuilder.forName(serverName)
            .directExecutor()
            .addService(new ChunkServerFileServiceImpl(fileChunkManager))
            .build()
            .start());

    blockingStub =
        ChunkServerFileServiceGrpc.newBlockingStub(
            // Create a server, add service, start, and register for automatic graceful shutdown
            grpcCleanupRule.register(
                InProcessChannelBuilder.forName(serverName).directExecutor().build()));
  }

  @Test
  public void chunkServerFileServiceImpl_initFileChunk() throws Exception {
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
}
