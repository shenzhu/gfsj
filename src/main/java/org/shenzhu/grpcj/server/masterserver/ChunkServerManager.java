package org.shenzhu.grpcj.server.masterserver;

import org.shenzhu.grpcj.protos.ChunkServerOuterClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ChunkServerManager {
  private final Logger logger = LoggerFactory.getLogger(getClass());

  /** Mapping chunk server location to actual chunk server. */
  private final ConcurrentHashMap<
          ChunkServerOuterClass.ChunkServerLocation, ChunkServerOuterClass.ChunkServer>
      chunkServerMap;

  /** Mapping chunk handle to chunk servers. */
  private final ConcurrentHashMap<String, Set<ChunkServerOuterClass.ChunkServerLocation>>
      chunkLocationsMap;

  /** Priority queue that stores a list of chunk servers. */
  private final PriorityQueue<ChunkServerOuterClass.ChunkServer> chunkServerHeap;

  /** Lock for chunk server heap. */
  private final Lock chunkServerHeapLock;

  /** Singleton object. */
  private static ChunkServerManager instance = null;

  /** Constructor. */
  public ChunkServerManager() {
    this.chunkServerMap = new ConcurrentHashMap<>();
    this.chunkLocationsMap = new ConcurrentHashMap<>();

    // Sort chunk servers by available disks
    this.chunkServerHeap =
        new PriorityQueue<ChunkServerOuterClass.ChunkServer>(
            new Comparator<ChunkServerOuterClass.ChunkServer>() {
              @Override
              public int compare(
                  ChunkServerOuterClass.ChunkServer o1, ChunkServerOuterClass.ChunkServer o2) {
                return o2.getAvailableDiskMb() - o1.getAvailableDiskMb();
              }
            });

    this.chunkServerHeapLock = new ReentrantLock();
  }

  /**
   * Get ChunkServerManager singleton instance.
   *
   * @return chunk server manager.
   */
  public static ChunkServerManager getInstance() {
    if (instance == null) {
      instance = new ChunkServerManager();
    }
    return instance;
  }

  /**
   * Allocate chunk servers for storing chunk handle.
   *
   * @param chunkHandle chunk handle
   * @param requestedServerNumber number of chunk servers requested
   * @return allocated chunk servers
   */
  public List<ChunkServerOuterClass.ChunkServerLocation> allocateChunkServer(
      String chunkHandle, int requestedServerNumber) {
    // First check if this chunk handle is already allocated
    if (this.chunkLocationsMap.containsKey(chunkHandle)) {
      logger.info("Chunk servers have already been allocation for chunk {}", chunkHandle);
      return new ArrayList<ChunkServerOuterClass.ChunkServerLocation>(
          this.chunkLocationsMap.get(chunkHandle));
    }

    // Use ConcurrentHashMap to create set to make the set thread-safe
    List<ChunkServerOuterClass.ChunkServerLocation> chunkServerLocations = new LinkedList<>();

    // Try to find chunk servers from heap, remember to lock
    this.chunkServerHeapLock.lock();

    logger.info("Trying to find chunk servers for chunk: {}", chunkHandle);
    int allocatedChunkServerCount = 0;
    List<ChunkServerOuterClass.ChunkServer> updatedChunkServers = new LinkedList<>();
    while (!this.chunkServerHeap.isEmpty()) {
      ChunkServerOuterClass.ChunkServer chunkServerCandidate = this.chunkServerHeap.peek();
      if (chunkServerCandidate.getAvailableDiskMb() < 10) {
        // Chunk servers are ordered by disk, if current chunk server doesn't have enough space,
        // just exit
        break;
      }

      this.chunkServerHeap.poll();
      ChunkServerOuterClass.ChunkServer.Builder chunkServerBuilder =
          ChunkServerOuterClass.ChunkServer.newBuilder(chunkServerCandidate);
      chunkServerBuilder.setAvailableDiskMb(chunkServerCandidate.getAvailableDiskMb() - 10);
      ChunkServerOuterClass.ChunkServer newChunkServer = chunkServerBuilder.build();

      // Store update chunk server information
      chunkServerLocations.add(newChunkServer.getLocation());
      updatedChunkServers.add(newChunkServer);

      allocatedChunkServerCount += 1;
      if (allocatedChunkServerCount >= requestedServerNumber) {
        break;
      }

      logger.info("Allocated chunk server {} for chunk {}", newChunkServer.toString(), chunkHandle);
    }
    this.chunkServerHeap.addAll(updatedChunkServers);
    this.chunkServerHeapLock.unlock();

    if (allocatedChunkServerCount > 0) {
      if (!this.chunkLocationsMap.containsKey(chunkHandle)) {
        this.chunkLocationsMap.put(chunkHandle, ConcurrentHashMap.newKeySet());
      }

      chunkServerLocations.forEach(
          loc -> {
            this.chunkLocationsMap.get(chunkHandle).add(loc);
          });
    }

    return chunkServerLocations;
  }

  /**
   * Register new chunk server.
   *
   * @param chunkServer chunk server
   * @return if registration succeeded
   */
  public boolean registerChunkServer(ChunkServerOuterClass.ChunkServer chunkServer) {
    logger.info("Registering new chunk server: {}", chunkServer.toString());

    // Add new chunk server to chunkServersMap
    this.chunkServerMap.put(chunkServer.getLocation(), chunkServer);

    // Add chunk handles in this chunk server
    for (String storedChunkHandle : chunkServer.getStoredChunkHandlesList()) {
      if (!this.chunkLocationsMap.containsKey(storedChunkHandle)) {
        this.chunkLocationsMap.put(storedChunkHandle, ConcurrentHashMap.newKeySet());
      }
      this.chunkLocationsMap.get(storedChunkHandle).add(chunkServer.getLocation());
    }

    // Add chunk server to chunkServerHeap
    this.chunkServerHeapLock.lock();
    this.chunkServerHeap.add(chunkServer);
    this.chunkServerHeapLock.unlock();

    return true;
  }

  /**
   * Unregister the chunk server at given location.
   *
   * @param chunkServerLocation chunk server location.
   */
  public void unRegisterChunkServer(ChunkServerOuterClass.ChunkServerLocation chunkServerLocation) {
    logger.info("Unregistering chunk server at location: {}", chunkServerLocation.toString());

    // Corner case, check if already removed
    if (!this.chunkServerMap.containsKey(chunkServerLocation)) {
      return;
    }

    // Get the actual chunk server
    ChunkServerOuterClass.ChunkServer chunkServer = this.chunkServerMap.get(chunkServerLocation);

    // Remove all chunk handles in this chunk server
    for (String storedChunkHandle : chunkServer.getStoredChunkHandlesList()) {
      if (this.chunkLocationsMap.containsKey(storedChunkHandle)) {
        this.chunkLocationsMap.get(storedChunkHandle).remove(chunkServerLocation);

        if (this.chunkLocationsMap.get(storedChunkHandle).size() == 0) {
          this.chunkLocationsMap.remove(storedChunkHandle);
        }
      }
    }

    // Remove from chunkServerMap
    this.chunkServerMap.remove(chunkServerLocation);

    // Remove from chunkServerHeap
    this.chunkServerHeapLock.lock();
    this.chunkServerHeap.remove(chunkServer);
    this.chunkServerHeapLock.unlock();
  }

  /** Unregister all chunk servers. */
  public void unRegisterAllChunkServers() {
    // Just clear every thing
    this.chunkServerMap.clear();
    this.chunkLocationsMap.clear();

    this.chunkServerHeapLock.lock();
    this.chunkServerHeap.clear();

    this.chunkServerHeapLock.unlock();
  }

  /**
   * Get chunk server at given location.
   *
   * @param chunkServerLocation chunk server location
   * @return chunk server
   */
  public ChunkServerOuterClass.ChunkServer getChunkServer(
      ChunkServerOuterClass.ChunkServerLocation chunkServerLocation) {
    logger.info("Retrieving chunk server at: {}", chunkServerLocation.toString());

    return this.chunkServerMap.get(chunkServerLocation);
  }

  /**
   * Update chunk server with given info.
   *
   * @param chunkServerLocation chunk server location
   * @param availableDisk available disk
   * @param chunksToAdd chunks to add
   * @param chunksToRemove chunks to remove
   */
  public void updateChunkServer(
      ChunkServerOuterClass.ChunkServerLocation chunkServerLocation,
      int availableDisk,
      Set<String> chunksToAdd,
      Set<String> chunksToRemove) {
    logger.info(
        "Updating chunk server at {} with disk space {}, chunks to add: {}, chunks to remove: {}",
        chunkServerLocation.toString(),
        availableDisk,
        chunksToAdd.toString(),
        chunksToRemove.toString());

    if (!this.chunkServerMap.containsKey(chunkServerLocation)) {
      return;
    }

    // Get its chunk server
    ChunkServerOuterClass.ChunkServer chunkServer = this.chunkServerMap.get(chunkServerLocation);

    // Update chunk location info
    for (String chunkToAdd : chunksToAdd) {
      if (!this.chunkLocationsMap.containsKey(chunkToAdd)) {
        this.chunkLocationsMap.put(chunkToAdd, ConcurrentHashMap.newKeySet());
      }
      this.chunkLocationsMap.get(chunkToAdd).add(chunkServerLocation);
    }

    for (String chunkToRemove : chunksToRemove) {
      if (!this.chunkLocationsMap.containsKey(chunkToRemove)) {
        continue;
      }

      Set<ChunkServerOuterClass.ChunkServerLocation> chunkLocations =
          this.chunkLocationsMap.get(chunkToRemove);
      chunkLocations.remove(chunkServerLocation);
      if (chunkLocations.size() == 0) {
        this.chunkLocationsMap.remove(chunkToRemove);
      }
    }

    // Create new chunk server with updated information
    ChunkServerOuterClass.ChunkServer.Builder chunkServerBuilder =
        ChunkServerOuterClass.ChunkServer.newBuilder();
    chunkServerBuilder.setLocation(chunkServerLocation);
    chunkServerBuilder.setAvailableDiskMb(availableDisk);

    for (String chunkHandle : chunkServer.getStoredChunkHandlesList()) {
      if (!chunksToRemove.contains(chunkHandle)) {
        chunkServerBuilder.addStoredChunkHandles(chunkHandle);
      }
    }

    for (String chunkHandle : chunksToAdd) {
      if (!chunksToRemove.contains(chunkHandle)) {
        chunkServerBuilder.addStoredChunkHandles(chunkHandle);
      }
    }

    ChunkServerOuterClass.ChunkServer newChunkServer = chunkServerBuilder.build();

    // Update chunkServerMap with new chunk server
    this.chunkServerMap.put(chunkServerLocation, newChunkServer);

    // Update chunkServerHeap with new chunk server
    this.chunkServerHeapLock.lock();
    this.chunkServerHeap.remove(chunkServer);
    this.chunkServerHeap.add(newChunkServer);

    this.chunkServerHeapLock.unlock();
  }

  /**
   * Get locations for given chunk handle.
   *
   * @param chunkHandle chunk handle
   * @return chunk server locations
   */
  public Set<ChunkServerOuterClass.ChunkServerLocation> getChunkLocations(String chunkHandle) {
    return this.chunkLocationsMap.get(chunkHandle);
  }
}
