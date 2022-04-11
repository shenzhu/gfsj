package org.shenzhu.grpcj.client;

import io.grpc.Channel;
import org.shenzhu.grpcj.protos.MasterMetadataServiceGrpc;
import org.shenzhu.grpcj.protos.MasterMetadataServiceOuterClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MasterMetadataServiceClient {
  private final Logger logger = LoggerFactory.getLogger(getClass());

  private MasterMetadataServiceGrpc.MasterMetadataServiceBlockingStub blockingStub;

  public MasterMetadataServiceClient(Channel channel) {
    this.blockingStub = MasterMetadataServiceGrpc.newBlockingStub(channel);
  }

  public MasterMetadataServiceOuterClass.OpenFileReply openFile(
      MasterMetadataServiceOuterClass.OpenFileRequest request) {
    logger.info("Will try sending OpenFileRequest to master server: {}", request);

    MasterMetadataServiceOuterClass.OpenFileReply reply = this.blockingStub.openFile(request);

    logger.info("Received OpenFileReply: {}", reply);

    return reply;
  }
}
