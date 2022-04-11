package org.shenzhu.grpcj.server.chunkserver;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteOptions;
import org.shenzhu.grpcj.protos.ChunkServerOuterClass;
import org.shenzhu.grpcj.protos.Metadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class FileChunkManager {
  private final Logger logger = LoggerFactory.getLogger(getClass());

  public final String DB_LOCATION = "D:\\playground\\gfsj\\data\\";

  // Status for chunk creation
  enum ChunkCreationStatus {
    ERROR, // failed to create chunk
    ALREADY_EXISTS, // chunk already exists
    OK, // success
  }

  // Max size in bytes for each chunk
  private int maxChunkSizeInBytes;

  // Underlying database for managing chunks in disk
  private RocksDB chunkDatabase;

  // Singleton
  private static FileChunkManager instance = null;

  /** Prevent calling constructor. */
  private FileChunkManager() {}

  /**
   * Singleton method for getting only instance of FileChunkManager.
   *
   * @return FileChunkManager instance
   */
  public static FileChunkManager getInstance() {
    if (instance == null) {
      instance = new FileChunkManager();
    }
    return instance;
  }

  /**
   * Initialize a RocksDB with given chunk database name and max chunk size.
   *
   * @param chunkDatabaseName database name
   * @param maxChunkSizeInBytes max chunk size in bytes
   */
  public void initialize(String chunkDatabaseName, int maxChunkSizeInBytes) {
    try {
      RocksDB.loadLibrary();

      Options options = new Options().setCreateIfMissing(true);

      this.chunkDatabase = RocksDB.open(options, DB_LOCATION + chunkDatabaseName);
      this.maxChunkSizeInBytes = maxChunkSizeInBytes;

    } catch (RocksDBException rocksDBException) {
      logger.error(
          "failed to open RocksDB {}, error: {}",
          DB_LOCATION + chunkDatabaseName,
          rocksDBException);
    }
  }

  /**
   * Write chunk handle and associataed chunk data to disk.
   *
   * @param chunkHandle chunk handle
   * @param fileChunk file chunk
   * @return if file chunk written succeeds
   */
  private boolean writeFileChunk(String chunkHandle, ChunkServerOuterClass.FileChunk fileChunk) {
    try {
      final WriteOptions writeOptions = new WriteOptions().setSync(true);
      this.chunkDatabase.put(
          writeOptions, chunkHandle.getBytes(StandardCharsets.UTF_8), fileChunk.toByteArray());

      return true;
    } catch (RocksDBException rocksDBException) {
      logger.error("Failed to write chunk {} to RocksDB, error: {}", chunkHandle, rocksDBException);

      return false;
    }
  }

  /**
   * Delete given chunk from disk.
   *
   * @param chunkHandle chunk handle
   * @return if deletion succeeded
   */
  public boolean deleteChunk(String chunkHandle) {
    try {
      final WriteOptions writeOptions = new WriteOptions().setSync(true);
      this.chunkDatabase.delete(writeOptions, chunkHandle.getBytes(StandardCharsets.UTF_8));

      return true;
    } catch (RocksDBException rocksDBException) {
      logger.error(
          "Failed to delete chunk {} from RocksDB, error: {}", chunkHandle, rocksDBException);

      return false;
    }
  }

  /**
   * Get all FileChunkMetadata.
   *
   * @return list of FileChunkMetadata
   */
  public List<Metadata.FileChunkMetadata> getAllFileChunkMetadata() {
    List<Metadata.FileChunkMetadata> metadatas = new ArrayList<>();

    // Iterate over all values in disk
    final RocksIterator iterator = this.chunkDatabase.newIterator();
    for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
      try {
        ChunkServerOuterClass.FileChunk currentChunk =
            ChunkServerOuterClass.FileChunk.parseFrom(iterator.value());

        // Extract FileChunkMetadata
        Metadata.FileChunkMetadata currFileChunkMetadata =
            Metadata.FileChunkMetadata.newBuilder()
                .setChunkHandle(new String(iterator.key()))
                .setVersion(currentChunk.getVersion())
                .build();

        metadatas.add(currFileChunkMetadata);
      } catch (InvalidProtocolBufferException invalidProtocolBufferException) {
        logger.error(
            "Failed to get chunk {}, error: {}",
            new String(iterator.key()),
            invalidProtocolBufferException);
      }
    }

    return metadatas;
  }

  /**
   * Get FileChunk specified by chunkHandle from RocksDB.
   *
   * @param chunkHandle chunk handle
   * @return Optional of FileChunk
   */
  public Optional<ChunkServerOuterClass.FileChunk> getFileChunk(String chunkHandle) {
    try {
      byte[] fileChunkBytes = this.chunkDatabase.get(chunkHandle.getBytes(StandardCharsets.UTF_8));
      if (fileChunkBytes == null) {
        return Optional.empty();
      }

      ChunkServerOuterClass.FileChunk fileChunk =
          ChunkServerOuterClass.FileChunk.parseFrom(fileChunkBytes);

      return Optional.of(fileChunk);
    } catch (RocksDBException rocksDBException) {
      logger.error("Failed to get chunk {} from RocksDB, error: {}", chunkHandle, rocksDBException);

      return Optional.empty();
    } catch (InvalidProtocolBufferException invalidProtocolBufferException) {
      logger.error(
          "Failed to get chunk {} due to ProtocolBuffer error {}",
          chunkHandle,
          invalidProtocolBufferException);

      return Optional.empty();
    }
  }

  /**
   * Get FileChunk specified by chunkHandle from RocksDB with given version.
   *
   * @param chunkHandle chunk handle
   * @param version chunk version
   * @return Optional of FileChunk
   */
  public Optional<ChunkServerOuterClass.FileChunk> getFileChunk(String chunkHandle, int version) {
    Optional<ChunkServerOuterClass.FileChunk> result = getFileChunk(chunkHandle);
    if (result.isEmpty()) {
      return Optional.empty();
    }

    if (result.get().getVersion() != version) {
      logger.error(
          "Failed to get FileChunk from RocksDB becasue of version mismatch, desired version {}, found version {}",
          version,
          result.get().getVersion());

      return Optional.empty();
    }

    return result;
  }

  /**
   * Get max chunk size in bytes.
   *
   * @return max chunk size in bytes
   */
  public int getMaxChunkSizeInBytes() {
    return this.maxChunkSizeInBytes;
  }

  /**
   * Create new chunk with given chunk handle and version if not already exists.
   *
   * @param chunkHandle chunk handle
   * @param version chunk version to create
   * @return ChunkCreationStatus
   */
  public ChunkCreationStatus createChunk(String chunkHandle, int version) {
    // First check if given chunk already exists
    RocksIterator iterator = this.chunkDatabase.newIterator();

    iterator.seek(chunkHandle.getBytes(StandardCharsets.UTF_8));
    if (iterator.isValid() && new String(iterator.key()).equals(chunkHandle)) {
      return ChunkCreationStatus.ALREADY_EXISTS;
    }

    // Create new chunk
    ChunkServerOuterClass.FileChunk newChunk =
        ChunkServerOuterClass.FileChunk.newBuilder().setVersion(version).build();

    boolean result = writeFileChunk(chunkHandle, newChunk);
    if (!result) {
      logger.error("Failed to create new chunk {}", chunkHandle);
      return ChunkCreationStatus.ERROR;
    }

    return ChunkCreationStatus.OK;
  }

  /**
   * Read data from chunk with given arguments.
   *
   * @param chunkHandle chunk handle
   * @param readVersion read version
   * @param startOffset start offset
   * @param length length
   * @return byte array
   */
  public byte[] readFromChunk(String chunkHandle, int readVersion, int startOffset, int length) {
    Optional<ChunkServerOuterClass.FileChunk> result = getFileChunk(chunkHandle, readVersion);
    if (result.isEmpty()) {
      return null;
    }

    ChunkServerOuterClass.FileChunk fileChunk = result.get();
    if (startOffset > fileChunk.getData().size()) {
      logger.error(
          "Failed to read chunk, start offset reaches limit, start offset {}, length {}",
          startOffset,
          length);
      return null;
    }

    return Arrays.copyOfRange(fileChunk.getData().toByteArray(), startOffset, startOffset + length);
  }

  /**
   * Write new data to chunk with given arguments.
   *
   * @param chunkHandle chunk handle
   * @param writeVersion write version
   * @param startOffset start offset in chunk
   * @param length write length
   * @param newData new data to write
   * @return write length
   */
  public int writeToChunk(
      String chunkHandle, int writeVersion, int startOffset, int length, String newData) {
    // Get FileChunk from RocksDB
    Optional<ChunkServerOuterClass.FileChunk> result = getFileChunk(chunkHandle, writeVersion);
    if (result.isEmpty()) {
      return 0;
    }

    // Check if start offset is valid
    ChunkServerOuterClass.FileChunk fileChunk = result.get();
    if (startOffset > fileChunk.getData().size()) {
      logger.error(
          "Failed to write chunk, start offset reaches limit, start offset {}, length {}",
          startOffset,
          fileChunk.getData().size());
    }

    // Check if still have remaining space
    int remainingBytes = this.maxChunkSizeInBytes - startOffset;
    if (remainingBytes <= 0) {
      logger.error(
          "Failed to write chunk due to insufficient bytes, remaining bytes {}, start offset {}",
          remainingBytes,
          startOffset);
      return 0;
    }

    // Get original data and replace
    byte[] data = fileChunk.getData().toByteArray();
    byte[] newByteData = newData.getBytes(StandardCharsets.UTF_8);

    int actualWriteLength = Math.min(length, remainingBytes);
    // Make sure there's enough space to write
    if (data.length < startOffset + actualWriteLength) {
      byte[] expandedData = new byte[startOffset + actualWriteLength];
      System.arraycopy(data, 0, expandedData, 0, data.length);
      data = expandedData;
    }
    System.arraycopy(newByteData, 0, data, startOffset, actualWriteLength);

    // Create new FileChunk and write back
    ChunkServerOuterClass.FileChunk newFileChunk =
        ChunkServerOuterClass.FileChunk.newBuilder()
            .setVersion(fileChunk.getVersion())
            .setData(ByteString.copyFrom(data))
            .build();
    boolean writeResult = writeFileChunk(chunkHandle, newFileChunk);
    if (!writeResult) {
      logger.error("Failed to write updated chunk to RocskDB");
      return 0;
    }

    return actualWriteLength;
  }

  /**
   * Update chunk file version.
   *
   * @param chunkHandle chunk handle
   * @param fromVersion original version
   * @param toVersion new version
   * @return if update succeeded
   */
  public boolean updateChunkVersion(String chunkHandle, int fromVersion, int toVersion) {
    Optional<ChunkServerOuterClass.FileChunk> result = getFileChunk(chunkHandle, fromVersion);
    if (result.isEmpty()) {
      return false;
    }

    // Create new FileChunk and write back
    ChunkServerOuterClass.FileChunk fileChunk = result.get();
    ChunkServerOuterClass.FileChunk newFileChunk =
        ChunkServerOuterClass.FileChunk.newBuilder()
            .setVersion(toVersion)
            .setData(fileChunk.getData())
            .build();

    boolean writeResult = writeFileChunk(chunkHandle, newFileChunk);
    if (!writeResult) {
      return false;
    }

    return true;
  }

  /**
   * Get the version of given chunk.
   *
   * @param chunkHandle chunk handle
   * @return Optional of version
   */
  public Optional<Integer> getChunkVersion(String chunkHandle) {
    Optional<ChunkServerOuterClass.FileChunk> result = getFileChunk(chunkHandle);
    if (result.isEmpty()) {
      return Optional.empty();
    }

    return Optional.of(result.get().getVersion());
  }

  /** Close RocksDB. */
  public void closeDatabase() {
    this.chunkDatabase.close();
  }
}
