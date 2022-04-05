package org.shenzhu.grpcj.server.masterserver;

import org.junit.Before;
import org.junit.Test;
import org.shenzhu.grpcj.protos.ChunkServerOuterClass;
import org.shenzhu.grpcj.protos.Metadata;

import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class MetadataManagerTest {

  private MetadataManager metadataManager;

  @Before
  public void SetUp() {
    this.metadataManager = MetadataManager.getInstance();
  }

  @Test
  public void testCreateSingleFileMetaData() {
    MetadataManager.MetadataCreateStatus status = this.metadataManager.createFileMetadata("\\foo");
    assertSame(status, MetadataManager.MetadataCreateStatus.OK);

    assertTrue(this.metadataManager.existFileMeta("\\foo"));
    Optional<Metadata.FileMetadata> fileMetadataOptional =
        this.metadataManager.getFileMetadata("\\foo");
    assertFalse(fileMetadataOptional.isEmpty());
    assertEquals(fileMetadataOptional.get().getFilename(), "\\foo");

    Optional<String> chunkHandle = this.metadataManager.createChunkHandle("\\foo", 0);
    assertFalse(chunkHandle.isEmpty());
    assertEquals(this.metadataManager.getFileMetadata("\\foo").get().getChunkHandlesCount(), 1);
  }

  @Test
  public void testCreateChunk() {
    MetadataManager.MetadataCreateStatus status =
        this.metadataManager.createFileMetadata("\\testcreatechunk");
    assertSame(status, MetadataManager.MetadataCreateStatus.OK);

    for (int i = 0; i < 5; ++i) {
      Optional<String> chunkHandle = this.metadataManager.createChunkHandle("\\testcreatechunk", i);
      assertFalse(chunkHandle.isEmpty());
    }

    Optional<Metadata.FileMetadata> fileMetadata =
        this.metadataManager.getFileMetadata("\\testcreatechunk");
    assertFalse(fileMetadata.isEmpty());

    Map<Integer, String> map = fileMetadata.get().getChunkHandlesMap();
    assertTrue(map.containsKey(0));
    assertTrue(map.containsKey(1));
    assertTrue(map.containsKey(2));
    assertTrue(map.containsKey(3));
    assertTrue(map.containsKey(4));
  }

  @Test
  public void testAdvanceChunkVersionAndSetPrimaryLocation() {
    String filename = "\\advanceChunkVersionAndSetPrimaryLocation";

    MetadataManager.MetadataCreateStatus status = this.metadataManager.createFileMetadata(filename);
    assertSame(status, MetadataManager.MetadataCreateStatus.OK);

    Optional<String> chunkHandleOptional = this.metadataManager.createChunkHandle(filename, 0);
    assertFalse(chunkHandleOptional.isEmpty());

    String chunkHandle = chunkHandleOptional.get();
    Metadata.FileChunkMetadata.Builder fileChunkMetadataBuilder =
        Metadata.FileChunkMetadata.newBuilder();
    fileChunkMetadataBuilder.setChunkHandle(chunkHandle);
    fileChunkMetadataBuilder.setVersion(0);

    ChunkServerOuterClass.ChunkServerLocation loc1 =
        ChunkServerOuterClass.ChunkServerLocation.newBuilder()
            .setServerHostname("localhost")
            .setServerPort(5000)
            .build();
    ChunkServerOuterClass.ChunkServerLocation loc2 =
        ChunkServerOuterClass.ChunkServerLocation.newBuilder()
            .setServerHostname("localhost")
            .setServerPort(5001)
            .build();
    ChunkServerOuterClass.ChunkServerLocation loc3 =
        ChunkServerOuterClass.ChunkServerLocation.newBuilder()
            .setServerHostname("localhost")
            .setServerPort(5002)
            .build();

    fileChunkMetadataBuilder.setPrimaryLocation(loc1);
    fileChunkMetadataBuilder.addLocations(loc2);
    fileChunkMetadataBuilder.addLocations(loc3);

    Metadata.FileChunkMetadata chunkData = fileChunkMetadataBuilder.build();

    assertEquals(chunkData.getVersion(), 0);
    this.metadataManager.setFileChunkMetadata(chunkData);

    // Advance chunk versions
    int versionAdvanceTimes = 5;
    for (int i = 0; i < versionAdvanceTimes; ++i) {
      boolean versionUpdate = this.metadataManager.advanceChunkVersion(chunkHandle);
      assertTrue(versionUpdate);
    }

    Optional<Metadata.FileChunkMetadata> fileChunkMetadataOptional =
        this.metadataManager.getFileChunkMetadata(chunkHandle);
    assertFalse(fileChunkMetadataOptional.isEmpty());
    assertEquals(fileChunkMetadataOptional.get().getVersion(), versionAdvanceTimes);
  }

  @Test
  public void testFileDelete() {
    String filename = "\\testFileDelete";
    assertFalse(this.metadataManager.existFileMeta(filename));

    MetadataManager.MetadataCreateStatus status = this.metadataManager.createFileMetadata(filename);
    assertSame(status, MetadataManager.MetadataCreateStatus.OK);
    assertTrue(this.metadataManager.existFileMeta(filename));

    this.metadataManager.deleteFileAndChunkMetadata(filename);
    assertFalse(this.metadataManager.existFileMeta(filename));
  }
}
