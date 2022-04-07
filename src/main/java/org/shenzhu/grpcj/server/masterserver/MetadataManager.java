package org.shenzhu.grpcj.server.masterserver;

import org.shenzhu.grpcj.protos.ChunkServerOuterClass;
import org.shenzhu.grpcj.protos.Metadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MetadataManager {
  private final Logger logger = LoggerFactory.getLogger(getClass());

  /** Thread-safe chunk id allocator. */
  private final AtomicLong globalChunkId;

  /** Thread-safe set for deleted chunk handles. */
  private final Set<String> deletedChunkHandles;

  /** Thread-safe hashmap mapping file to its metadata. */
  private final ConcurrentHashMap<String, Metadata.FileMetadata> fileMetadata;

  /** Thread-safe hashmap mapping chunk handle to its metadata. */
  private final ConcurrentHashMap<String, Metadata.FileChunkMetadata> chunkMetadata;

  /** Thread-safe hashmap mapping chunk handle to its location and lease expiration time. */
  private final ConcurrentHashMap<
          String, Map.Entry<ChunkServerOuterClass.ChunkServerLocation, Long>>
      leaseHolders;

  /** Lock manager. */
  private final LockManager lockManager;

  /** MetadataCreateStatus */
  public enum MetadataCreateStatus {
    OK,
    FAILED,
    ALREADY_EXISTS
  }

  /** Singleton. */
  private static MetadataManager instance = null;

  /** Constructor. */
  public MetadataManager() {
    this.globalChunkId = new AtomicLong(0);

    this.deletedChunkHandles = ConcurrentHashMap.newKeySet();
    this.fileMetadata = new ConcurrentHashMap<>();
    this.chunkMetadata = new ConcurrentHashMap<>();
    this.leaseHolders = new ConcurrentHashMap<>();

    lockManager = LockManager.getInstance();
  }

  /**
   * Get singleton instance.
   *
   * @return singleton
   */
  public static MetadataManager getInstance() {
    if (instance == null) {
      instance = new MetadataManager();
    }
    return instance;
  }

  /**
   * Create file metadata for given file name.
   *
   * @param filename file name
   * @return MetadataCreateStatus
   */
  public MetadataCreateStatus createFileMetadata(String filename) {
    // Step 1. Use read lock to lock all parent directory
    FileLockManager fileLockManager = new FileLockManager(filename, this.lockManager);
    boolean pathLockStatus = fileLockManager.acquireLock();
    if (!pathLockStatus) {
      logger.error("Failed to acquire parent lock for file {}", filename);
      return MetadataCreateStatus.FAILED;
    }

    // Step 2. Add write lock for current file
    ReentrantReadWriteLock writeLock = this.lockManager.createLock(filename);
    writeLock.writeLock().lock();

    // Step 3. Create a FileMetadata object
    if (this.fileMetadata.containsKey(filename)) {
      logger.error("Failed to create FileMetadata, already exists for {}", filename);
      return MetadataCreateStatus.ALREADY_EXISTS;
    }

    Metadata.FileMetadata.Builder metadataBuilder = Metadata.FileMetadata.newBuilder();
    metadataBuilder.setFilename(filename);
    this.fileMetadata.put(filename, metadataBuilder.build());

    // Remember to release lock
    writeLock.writeLock().unlock();
    fileLockManager.releaseLock();

    logger.info("Successfully created file metadata for {}", filename);
    return MetadataCreateStatus.OK;
  }

  /**
   * Check if file metadata exists.
   *
   * @param filename file name
   * @return if metadata exists
   */
  public boolean existFileMeta(String filename) {
    return this.fileMetadata.containsKey(filename);
  }

  /**
   * Get file metadata.
   *
   * @param filename file name
   * @return file metadata
   */
  public Optional<Metadata.FileMetadata> getFileMetadata(String filename) {
    if (!this.fileMetadata.containsKey(filename)) {
      return Optional.empty();
    }

    return Optional.of(this.fileMetadata.get(filename));
  }

  /**
   * Create new chunk handle in given file at given position.
   *
   * @param filename file name
   * @param chunkIndex chunk index
   * @return created chunk handle
   */
  public Optional<String> createChunkHandle(String filename, int chunkIndex) {
    // Step 1. readLock on parent directories
    FileLockManager fileLockManager = new FileLockManager(filename, this.lockManager);
    boolean pathLockStatus = fileLockManager.acquireLock();
    if (!pathLockStatus) {
      logger.error("Failed to acquire parent lock for file {}", filename);
      return Optional.empty();
    }

    // Step 2. writeLock for current path
    ReentrantReadWriteLock writeLock = this.lockManager.createLock(filename);
    writeLock.writeLock().lock();

    // Step 3. fetch file metadata
    Optional<Metadata.FileMetadata> fileMetadata = getFileMetadata(filename);
    if (fileMetadata.isEmpty()) {
      logger.error("Failed to get file metadata for file {}", filename);
      return Optional.empty();
    }

    // Step 4. get new chunk handle and insert data
    String newChunkHandle = allocateNewChunkHandle();
    Map<Integer, String> chunkHandles = fileMetadata.get().getChunkHandlesMap();
    if (chunkHandles.containsKey(chunkIndex)) {
      logger.error(
          "Failed to create chunk handle, already exists in {} at {}", filename, chunkIndex);
      return Optional.empty();
    }

    // Step 5. create new FileMetadata and put back
    Metadata.FileMetadata.Builder newFileMetadataBuilder = Metadata.FileMetadata.newBuilder();
    newFileMetadataBuilder.setFilename(filename);
    newFileMetadataBuilder.putAllChunkHandles(chunkHandles);
    newFileMetadataBuilder.putChunkHandles(chunkIndex, newChunkHandle);
    this.fileMetadata.put(filename, newFileMetadataBuilder.build());

    // Step 6. update FileChunkMetadata
    Metadata.FileChunkMetadata.Builder newFileChunkMetadataBuilder =
        Metadata.FileChunkMetadata.newBuilder();
    newFileChunkMetadataBuilder.setChunkHandle(newChunkHandle);
    setFileChunkMetadata(newFileChunkMetadataBuilder.build());

    // Remember to unlock
    writeLock.writeLock().unlock();
    fileLockManager.releaseLock();

    return Optional.of(newChunkHandle);
  }

  /**
   * Get chunk handle in given file at given index.
   *
   * @param filename file name
   * @param chunkIndex chunk index
   * @return chunk handle
   */
  public Optional<String> getChunkHandle(String filename, int chunkIndex) {
    // Step 1. readLock on path
    FileLockManager fileLockManager = new FileLockManager(filename, this.lockManager);
    boolean pathLockStatus = fileLockManager.acquireLock();
    if (!pathLockStatus) {
      logger.error("Failed to acquire parent lock for file {}", filename);
      return Optional.empty();
    }

    // Step 2. read lock current path
    Optional<ReentrantReadWriteLock> readLock = this.lockManager.fetchLock(filename);
    if (readLock.isEmpty()) {
      logger.error("Failed to fetch read lock for file: {}", filename);
      return Optional.empty();
    }
    readLock.get().readLock().lock();

    // Step 3. get file metadata
    Optional<Metadata.FileMetadata> fileMetadata = getFileMetadata(filename);
    if (fileMetadata.isEmpty()) {
      logger.error("Failed to fetch FileMetadata for {}", filename);
      return Optional.empty();
    }

    // Step 4. fetch chunk handle
    Map<Integer, String> chunkHandleMap = fileMetadata.get().getChunkHandlesMap();
    if (!chunkHandleMap.containsKey(chunkIndex)) {
      logger.error("Failed to fetch chunk handle from FileMetadata {} at {}", filename, chunkIndex);
      return Optional.empty();
    }

    // Remember to release lock
    readLock.get().readLock().unlock();
    fileLockManager.releaseLock();

    return Optional.of(chunkHandleMap.get(chunkIndex));
  }

  /**
   * Advance the version of given chunk handle.
   *
   * @param chunkHandle chunk handle
   * @return if version update succeeded
   */
  public boolean advanceChunkVersion(String chunkHandle) {
    Optional<Metadata.FileChunkMetadata> fileChunkMetadata = getFileChunkMetadata(chunkHandle);
    if (fileChunkMetadata.isEmpty()) {
      return false;
    }

    // Create new FileChunkMetadata to replace the old one
    Metadata.FileChunkMetadata.Builder builder = Metadata.FileChunkMetadata.newBuilder();
    builder.setChunkHandle(chunkHandle);
    builder.setVersion(fileChunkMetadata.get().getVersion() + 1);
    setFileChunkMetadata(builder.build());

    return true;
  }

  /**
   * Get file chunk metadata for given chunk handle.
   *
   * @param chunkHandle chunk handle
   * @return FileChunkMetadata
   */
  public Optional<Metadata.FileChunkMetadata> getFileChunkMetadata(String chunkHandle) {
    if (!this.chunkMetadata.containsKey(chunkHandle)) {
      return Optional.empty();
    }
    return Optional.of(this.chunkMetadata.get(chunkHandle));
  }

  /**
   * Set file chunk metadata.
   *
   * @param fileChunkMetadata FileChunkMetadata
   */
  public void setFileChunkMetadata(Metadata.FileChunkMetadata fileChunkMetadata) {
    String chunkHandle = fileChunkMetadata.getChunkHandle();
    chunkMetadata.put(chunkHandle, fileChunkMetadata);
  }

  /**
   * Delete file chunk metadata.
   *
   * @param chunkHandle FileChunkMetadata
   */
  public void deleteFileChunkMetadata(String chunkHandle) {
    chunkMetadata.remove(chunkHandle);
  }

  /**
   * Set primary lease metadata.
   *
   * @param chunkHandle chunk handle
   * @param serverLocation server location
   * @param expirationUnixSec expiration time
   */
  public void setPrimaryLeaseMetadata(
      String chunkHandle,
      ChunkServerOuterClass.ChunkServerLocation serverLocation,
      long expirationUnixSec) {
    this.leaseHolders.put(
        chunkHandle,
        new AbstractMap.SimpleEntry<ChunkServerOuterClass.ChunkServerLocation, Long>(
            serverLocation, expirationUnixSec));
  }

  /**
   * Get primary lease metadata.
   *
   * @param chunkHandle chunk handle
   * @return primary chunk server location and expiration time
   */
  public Optional<Map.Entry<ChunkServerOuterClass.ChunkServerLocation, Long>>
      getPrimaryLeaseMetadata(String chunkHandle) {
    if (!this.leaseHolders.containsKey(chunkHandle)) {
      return Optional.empty();
    }
    return Optional.of(this.leaseHolders.get(chunkHandle));
  }

  /**
   * Delete FileMetadata for given filename and all related FileChunkMetadata.
   *
   * @param filename file name
   */
  public void deleteFileAndChunkMetadata(String filename) {
    // Step 1. Use read lock to lock all parent directory
    FileLockManager fileLockManager = new FileLockManager(filename, this.lockManager);
    boolean pathLockStatus = fileLockManager.acquireLock();
    if (!pathLockStatus) {
      return;
    }

    // Step 2. write lock on current path
    ReentrantReadWriteLock writeLock = this.lockManager.createLock(filename);
    writeLock.writeLock().lock();

    // Step 3. fetch file metadata
    Optional<Metadata.FileMetadata> fileMetadata = getFileMetadata(filename);
    if (fileMetadata.isEmpty()) {
      return;
    }

    // Step 4. delete all chunk handle metadata
    fileMetadata
        .get()
        .getChunkHandlesMap()
        .forEach(
            (chunkIndex, chunkHandle) -> {
              deleteFileChunkMetadata(chunkHandle);
            });

    // Step 5. delete FileMetadata
    this.fileMetadata.remove(filename);

    // Remember to release lock
    writeLock.writeLock().unlock();
    fileLockManager.releaseLock();
  }

  /**
   * Allocate new chunk handle value.
   *
   * @return chunk handle
   */
  public String allocateNewChunkHandle() {
    return String.valueOf(this.globalChunkId.getAndIncrement());
  }

  /**
   * Remove lease for chunk handle.
   *
   * @param chunkHandle chunk handle
   */
  public void removePrimaryLeaseMetadata(String chunkHandle) {
    this.leaseHolders.remove(chunkHandle);
  }
}
