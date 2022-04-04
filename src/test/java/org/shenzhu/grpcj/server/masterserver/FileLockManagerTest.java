package org.shenzhu.grpcj.server.masterserver;

import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class FileLockManagerTest {

  private LockManager lockManager;

  @Before
  public void SetUp() {
    this.lockManager = LockManager.getInstance();
  }

  @Test
  public void testAddLock() {
    ReentrantReadWriteLock fooLock = this.lockManager.createLock("\\foo");
    assertNotNull(fooLock);
    assertTrue(this.lockManager.exists("\\foo"));
  }

  @Test
  public void testFileLockManager() {
    String filename = "\\foo\\bar\\baz";
    FileLockManager fileLockManager = new FileLockManager(filename, this.lockManager);

    this.lockManager.createLock("");
    this.lockManager.createLock("\\foo");
    this.lockManager.createLock("\\foo\\bar");
    boolean lockAcquireStatus = fileLockManager.acquireLock();
    assertTrue(lockAcquireStatus);
    assertEquals(fileLockManager.lockSize(), 3);

    fileLockManager.releaseLock();
    assertEquals(fileLockManager.lockSize(), 0);
  }
}
