package org.shenzhu.grpcj.server.masterserver;

import org.junit.Before;
import org.junit.Test;
import org.shenzhu.grpcj.protos.Metadata;

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
    assertEquals(chunkHandle.get(), "0");
    assertEquals(this.metadataManager.getFileMetadata("\\foo").get().getChunkHandlesCount(), 1);
  }
}
