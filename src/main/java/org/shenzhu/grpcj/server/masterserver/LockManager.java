package org.shenzhu.grpcj.server.masterserver;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LockManager {

  /** Singleton. */
  private static LockManager instance = null;

  /** Locks for different file path. */
  private final ConcurrentHashMap<String, ReentrantReadWriteLock> filePathLocks;

  /** Constructor. */
  public LockManager() {
    this.filePathLocks = new ConcurrentHashMap<>();
  }

  /**
   * Singleton method to get LockManager.
   *
   * @return singleton instance
   */
  public static LockManager getInstance() {
    if (instance == null) {
      instance = new LockManager();
    }

    return instance;
  }

  /**
   * Create lock for specified file name, get one if already exists.
   *
   * @param fileName file name
   * @return lock
   */
  public ReentrantReadWriteLock createLock(String fileName) {
    if (!this.filePathLocks.containsKey(fileName)) {
      this.filePathLocks.put(fileName, new ReentrantReadWriteLock());
    }

    return this.filePathLocks.get(fileName);
  }

  /**
   * Fetch lock for given file name.
   *
   * @param fileName file name
   * @return lock
   */
  public Optional<ReentrantReadWriteLock> fetchLock(String fileName) {
    if (!this.filePathLocks.containsKey(fileName)) {
      return Optional.empty();
    }

    return Optional.of(this.filePathLocks.get(fileName));
  }

  /**
   * If lock for file name exists.
   *
   * @param fileName file name
   * @return if lock exists
   */
  public boolean exists(String fileName) {
    return this.filePathLocks.containsKey(fileName);
  }
}
