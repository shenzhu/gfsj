package org.shenzhu.grpcj.server.chunkserver;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.shenzhu.grpcj.protos.Metadata;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class FileChunkManagerTest {
  private FileChunkManager fileChunkManager;

  @Before
  public void SetUp() {
    fileChunkManager = FileChunkManager.getInstance();
    fileChunkManager.initialize("fileChunkManagerTestDB", 1000);
  }

  @After
  public void clean() {
    fileChunkManager.closeDatabase();
  }

  @Test
  public void basicCRUDTest() {
    String data = "Testing the filechunkmanager";
    String handle = "BasicCrudOperationTest";

    // clear data if exists
    fileChunkManager.deleteChunk(handle);

    // create v1 of chunk
    int version = 1;
    FileChunkManager.ChunkCreationStatus creationStatus =
        fileChunkManager.createChunk(handle, version);
    assertTrue(
        (creationStatus == FileChunkManager.ChunkCreationStatus.OK)
            || (creationStatus == FileChunkManager.ChunkCreationStatus.ALREADY_EXISTS));

    // bump version before write
    assertTrue(fileChunkManager.updateChunkVersion(handle, version, version + 1));
    version++;

    // write new data to chunk
    int writeSize = fileChunkManager.writeToChunk(handle, version, 0, data.length(), data);

    // verify that all data was written
    assertEquals(data.length(), writeSize);

    // read the data
    int readStartOffset = 5;
    int readLength = 7;
    byte[] readResult =
        fileChunkManager.readFromChunk(handle, version, readStartOffset, readLength);
    assertEquals(
        data.substring(readStartOffset, readStartOffset + readLength), new String(readResult));

    // bump the chunk version
    assertTrue(fileChunkManager.updateChunkVersion(handle, version, version + 1));
    version++;

    // make another write at a different offset
    int updateOffset = 10;
    String updateData = " Updating the previous write.";
    int updateSize =
        fileChunkManager.writeToChunk(
            handle, version, updateOffset, updateData.length(), updateData);

    assertEquals(updateData.length(), updateSize);

    // read the last data we wrote
    byte[] updateReadResult =
        fileChunkManager.readFromChunk(handle, version, updateOffset, updateData.length());

    // verify chunk version
    assertEquals(updateData, new String(updateReadResult));

    // verify chunk version
    assertEquals(version, fileChunkManager.getChunkVersion(handle));

    assertTrue(fileChunkManager.deleteChunk(handle));

    // can't read deleted chunk
    assertNull(fileChunkManager.readFromChunk(handle, version, updateOffset, updateData.length()));
  }

  @Test
  public void testGetAllFileChunkMetadata() {
    String data = "Testing the filechunkmanager.";
    String handlePrefix = "GetAllFileChunkMetadata";
    int chunkCount = 10;

    Set<String> chunkHandles = new HashSet<>();
    for (int chunkId = 0; chunkId < chunkCount; ++chunkId) {
      int version = 1;
      String handle = handlePrefix + String.valueOf(chunkId);

      fileChunkManager.deleteChunk(handle);
      FileChunkManager.ChunkCreationStatus creationStatus =
          fileChunkManager.createChunk(handle, version);

      assertTrue(
          (creationStatus == FileChunkManager.ChunkCreationStatus.ALREADY_EXISTS)
              || (creationStatus == FileChunkManager.ChunkCreationStatus.OK));
      assertTrue(fileChunkManager.updateChunkVersion(handle, version, version + 1));
      version++;

      int writeSize = fileChunkManager.writeToChunk(handle, version, 0, data.length(), data);
      assertEquals(data.length(), writeSize);

      chunkHandles.add(handle);
    }

    List<Metadata.FileChunkMetadata> allChunkMetas = fileChunkManager.getAllFileChunkMetadata();

    assertEquals(allChunkMetas.size(), chunkCount);

    for (Metadata.FileChunkMetadata fileChunkMetadata : allChunkMetas) {
      assertTrue(chunkHandles.remove(fileChunkMetadata.getChunkHandle()));
    }
  }
}
