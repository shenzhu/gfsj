package org.shenzhu.grpcj.server.masterserver;

import org.junit.Before;
import org.junit.Test;
import org.shenzhu.grpcj.protos.ChunkServerOuterClass;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ChunkServerManagerTest {
  private ChunkServerManager chunkServerManager;

  private ChunkServerOuterClass.ChunkServerLocation makeChunkServerLocation(String host, int port) {
    ChunkServerOuterClass.ChunkServerLocation.Builder builder =
        ChunkServerOuterClass.ChunkServerLocation.newBuilder();
    builder.setServerHostname(host);
    builder.setServerPort(port);

    return builder.build();
  }

  private ChunkServerOuterClass.ChunkServer.Builder makeChunkServerBuilder(
      ChunkServerOuterClass.ChunkServerLocation chunkServerLocation) {
    ChunkServerOuterClass.ChunkServer.Builder builder =
        ChunkServerOuterClass.ChunkServer.newBuilder();
    builder.setLocation(chunkServerLocation);

    return builder;
  }

  @Before
  public void SetUp() {
    this.chunkServerManager = ChunkServerManager.getInstance();
    this.chunkServerManager.unRegisterAllChunkServers();
  }

  @Test
  public void testRegisterEmptyChunkServer() {
    ChunkServerOuterClass.ChunkServerLocation chunkServerLocation =
        makeChunkServerLocation("localhost", 5000);
    ChunkServerOuterClass.ChunkServer chunkServer =
        makeChunkServerBuilder(chunkServerLocation).build();

    assertTrue(this.chunkServerManager.registerChunkServer(chunkServer));
    assertEquals(chunkServer, this.chunkServerManager.getChunkServer(chunkServerLocation));
  }

  @Test
  public void testRegisterUnregisterChunkServerWithChunks() {
    // Prepare chunk server location and chunk server
    ChunkServerOuterClass.ChunkServerLocation chunkServerLocation =
        makeChunkServerLocation("localhost", 5000);

    ChunkServerOuterClass.ChunkServer.Builder chunkServerBuilder =
        makeChunkServerBuilder(chunkServerLocation);
    chunkServerBuilder.addStoredChunkHandles("chunk1");
    chunkServerBuilder.addStoredChunkHandles("chunk2");
    ChunkServerOuterClass.ChunkServer chunkServer = chunkServerBuilder.build();

    // Register chunk server
    assertTrue(this.chunkServerManager.registerChunkServer(chunkServer));
    assertEquals(chunkServer, this.chunkServerManager.getChunkServer(chunkServerLocation));

    Set<ChunkServerOuterClass.ChunkServerLocation> chunk1Locs =
        this.chunkServerManager.getChunkLocations("chunk1");
    assertEquals(chunk1Locs.size(), 1);
    assertTrue(chunk1Locs.contains(chunkServerLocation));

    Set<ChunkServerOuterClass.ChunkServerLocation> chunk2Locs =
        this.chunkServerManager.getChunkLocations("chunk2");
    assertEquals(chunk2Locs.size(), 1);
    assertTrue(chunk2Locs.contains(chunkServerLocation));

    // Unregister
    this.chunkServerManager.unRegisterChunkServer(chunkServerLocation);
    assertNull(this.chunkServerManager.getChunkServer(chunkServerLocation));
    assertNull(this.chunkServerManager.getChunkLocations("chunk1"));
    assertNull(this.chunkServerManager.getChunkLocations("chunk2"));
  }

  @Test
  public void testAllocateChunkServer() {
    // Prepare chunk server location and chunk server
    ChunkServerOuterClass.ChunkServerLocation loc1 = makeChunkServerLocation("localhost", 5000);
    ChunkServerOuterClass.ChunkServerLocation loc2 = makeChunkServerLocation("localhost", 5001);

    ChunkServerOuterClass.ChunkServer.Builder builder1 = makeChunkServerBuilder(loc1);
    builder1.setAvailableDiskMb(100);
    ChunkServerOuterClass.ChunkServer.Builder builder2 = makeChunkServerBuilder(loc2);
    builder2.setAvailableDiskMb(50);

    ChunkServerOuterClass.ChunkServer chunkServer1 = builder1.build();
    ChunkServerOuterClass.ChunkServer chunkServer2 = builder2.build();

    // Register
    assertTrue(this.chunkServerManager.registerChunkServer(chunkServer1));
    assertTrue(this.chunkServerManager.registerChunkServer(chunkServer2));

    Set<ChunkServerOuterClass.ChunkServerLocation> allocatedChunkServerLocs =
        this.chunkServerManager.allocateChunkServer("chunk1", 2);

    assertEquals(2, allocatedChunkServerLocs.size());
    assertTrue(allocatedChunkServerLocs.contains(loc1));
    assertTrue(allocatedChunkServerLocs.contains(loc2));
  }

  @Test
  public void updateChunkServer() {
    ChunkServerOuterClass.ChunkServerLocation chunkServerLocation =
        makeChunkServerLocation("localhost", 5000);
    ChunkServerOuterClass.ChunkServer.Builder builder1 =
        makeChunkServerBuilder(chunkServerLocation);

    for (int i = 0; i < 3; ++i) {
      builder1.addStoredChunkHandles(String.valueOf(i));
    }

    ChunkServerOuterClass.ChunkServer chunkServer = builder1.build();

    assertTrue(this.chunkServerManager.registerChunkServer(chunkServer));
    assertEquals(chunkServer, this.chunkServerManager.getChunkServer(chunkServerLocation));

    Set<String> chunksToRemove = ConcurrentHashMap.newKeySet();
    chunksToRemove.add("1");
    Set<String> chunksToAdd = ConcurrentHashMap.newKeySet();
    chunksToAdd.add("3");
    chunksToAdd.add("4");

    this.chunkServerManager.updateChunkServer(
        chunkServerLocation, 100, chunksToAdd, chunksToRemove);

    assertEquals(
        this.chunkServerManager.getChunkServer(chunkServerLocation).getAvailableDiskMb(), 100);
    assertEquals(
        this.chunkServerManager.getChunkServer(chunkServerLocation).getStoredChunkHandlesCount(),
        4);

    Set<String> storedChunks =
        new HashSet<>(
            this.chunkServerManager
                .getChunkServer(chunkServerLocation)
                .getStoredChunkHandlesList());
    assertEquals(storedChunks.size(), 4);
    assertTrue(storedChunks.contains("0"));
    assertTrue(storedChunks.contains("2"));
    assertTrue(storedChunks.contains("3"));
    assertTrue(storedChunks.contains("4"));
  }
}
