package org.shenzhu.grpcj.server.chunkserver;

import java.util.concurrent.ConcurrentHashMap;

public class ChunkDataCacheManager {
  /** Concurrent hashmap for data cache. */
  private final ConcurrentHashMap<String, String> dataCache;

  /** Singleton ChunkDataCacheManager. */
  private static ChunkDataCacheManager instance = null;

  /** Disable constructor. */
  private ChunkDataCacheManager() {
    dataCache = new ConcurrentHashMap<>();
  }

  /**
   * Singleton method for getting only instance of ChunkDataCacheManager.
   *
   * @return instance
   */
  public static ChunkDataCacheManager getInstance() {
    if (instance == null) {
      instance = new ChunkDataCacheManager();
    }
    return instance;
  }

  /**
   * Get value from data cache.
   *
   * @param key key
   * @return value
   */
  public String getValue(String key) {
    return this.dataCache.get(key);
  }

  /**
   * Set value in data cache.
   *
   * @param key key
   * @param value value
   */
  public void setValue(String key, String value) {
    this.dataCache.put(key, value);
  }

  /**
   * Remove key-value from data cache.
   *
   * @param key key
   * @return previous value
   */
  public String removeValue(String key) {
    return this.dataCache.remove(key);
  }
}
