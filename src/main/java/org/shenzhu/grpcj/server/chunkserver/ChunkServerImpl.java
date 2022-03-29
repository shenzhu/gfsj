package org.shenzhu.grpcj.server.chunkserver;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class ChunkServerImpl {
  /** File chunk manager. */
  private final FileChunkManager fileChunkManager;

  /** Hashmap mapping chunk handle to its version. */
  private final ConcurrentHashMap<String, Integer> chunkVersions;

  /** Hashmap mapping chunk handle to its lease expiration time. */
  private final ConcurrentHashMap<String, Long> chunkLeaseExpirationTime;

  public ChunkServerImpl(FileChunkManager fileChunkManager) {
    this.fileChunkManager = fileChunkManager;

    this.chunkVersions = new ConcurrentHashMap<>();
    this.chunkLeaseExpirationTime = new ConcurrentHashMap<>();
  }

  /**
   * Remove lease for chunk handle.
   *
   * @param chunkHandle chunk handle
   */
  public void removeLease(String chunkHandle) {
    this.chunkLeaseExpirationTime.remove(chunkHandle);
  }

  /**
   * Get lease expiration time for chunk handle.
   *
   * @param chunkHandle chunk handle
   * @return Optional of expiration time
   */
  public Optional<Long> getLeaseExpirationTime(String chunkHandle) {
    if (!this.chunkLeaseExpirationTime.containsKey(chunkHandle)) {
      return Optional.empty();
    } else {
      return Optional.of(this.chunkLeaseExpirationTime.get(chunkHandle));
    }
  }

  /**
   * Add or update lease expiration time for chunk handle.
   *
   * @param chunkHandle chunk handle
   * @param expirationTime lease expiration time
   */
  public void addOrUpdateLease(String chunkHandle, long expirationTime) {
    this.chunkLeaseExpirationTime.put(chunkHandle, expirationTime);
  }

  /**
   * Get the chunk version of given chunk handle.
   *
   * @param chunkHandle chunk handle
   * @return Optional of integer
   */
  public Optional<Integer> getChunkVersion(String chunkHandle) {
    return this.fileChunkManager.getChunkVersion(chunkHandle);
  }
}
