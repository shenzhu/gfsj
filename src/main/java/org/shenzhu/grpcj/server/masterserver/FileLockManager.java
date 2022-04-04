package org.shenzhu.grpcj.server.masterserver;

import java.util.Optional;
import java.util.Stack;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class FileLockManager {
  /** File path. */
  private final String filePath;

  /** Lock manager. */
  private final LockManager lockManager;

  /** Stack of locks. */
  private final Stack<ReentrantReadWriteLock> locks;

  /**
   * Constructor for file lock manager.
   *
   * @param filepath file path
   * @param lockManager lock manager
   */
  public FileLockManager(String filepath, LockManager lockManager) {
    this.filePath = filepath;
    this.lockManager = lockManager;

    this.locks = new Stack<>();
  }

  /**
   * Acquire lock for provided path.
   *
   * @return if lock accusation succeeded
   */
  public boolean acquireLock() {
    // Detect OS to find proper delimiter
    String osName = System.getProperty("os.name").toLowerCase();
    String delimiter = osName.startsWith("windows") ? "\\" : "/";

    // Try acquiring all read locks in path
    int delimiterPos = this.filePath.indexOf(delimiter);
    while (delimiterPos != -1) {
      String currFilePath = filePath.substring(0, delimiterPos);
      Optional<ReentrantReadWriteLock> currLock = this.lockManager.fetchLock(currFilePath);
      if (currLock.isEmpty()) {
        return false;
      }

      currLock.get().readLock().lock();
      this.locks.push(currLock.get());

      delimiterPos = this.filePath.indexOf(delimiter, delimiterPos + 1);
    }

    return true;
  }

  /** Release all locks in path. */
  public void releaseLock() {
    while (!this.locks.isEmpty()) {
      ReentrantReadWriteLock lock = this.locks.pop();
      lock.readLock().unlock();
    }
  }

  /**
   * Get read locks acquired now.
   *
   * @return number of locks
   */
  public int lockSize() {
    return this.locks.size();
  }
}
