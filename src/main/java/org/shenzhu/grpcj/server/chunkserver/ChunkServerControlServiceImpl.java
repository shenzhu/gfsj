package org.shenzhu.grpcj.server.chunkserver;

import org.shenzhu.grpcj.protos.ChunkServerControlServiceGrpc;
import org.shenzhu.grpcj.protos.ChunkServerControlServiceOuterClass;

public class ChunkServerControlServiceImpl
    extends ChunkServerControlServiceGrpc.ChunkServerControlServiceImplBase {

  /** Chunk server implementation. */
  private final ChunkServerImpl chunkServer;

  public ChunkServerControlServiceImpl(ChunkServerImpl chunkServer) {
    this.chunkServer = chunkServer;
  }

  @Override
  public void checkHeartBeat(
      org.shenzhu.grpcj.protos.ChunkServerControlServiceOuterClass.CheckHeartBeatRequest request,
      io.grpc.stub.StreamObserver<
              org.shenzhu.grpcj.protos.ChunkServerControlServiceOuterClass.CheckHeartBeatReply>
          responseObserver) {
    ChunkServerControlServiceOuterClass.CheckHeartBeatReply.Builder replyBuilder =
        ChunkServerControlServiceOuterClass.CheckHeartBeatReply.newBuilder();
    replyBuilder.setRequest(request);

    responseObserver.onNext(replyBuilder.build());
    responseObserver.onCompleted();
  }
}
