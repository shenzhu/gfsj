package org.shenzhu.grpcj.client;

import io.grpc.Channel;
import org.shenzhu.grpcj.protos.ChunkServerLeaseServiceGrpc;

public class ChunkServerLeaseServiceClient {
  /** Stub to communicate with server. */
  private ChunkServerLeaseServiceGrpc.ChunkServerLeaseServiceBlockingStub blockingStub;

  /**
   * Constructor.
   *
   * @param channel grpc channel
   */
  public ChunkServerLeaseServiceClient(Channel channel) {
    this.blockingStub = ChunkServerLeaseServiceGrpc.newBlockingStub(channel);
  }
}
