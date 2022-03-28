package org.shenzhu.grpcj;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.nio.charset.StandardCharsets;

public class Hello {
  public static void main(String[] args) {
    System.out.println("hello world");

    RocksDB.loadLibrary();

    try (final Options options = new Options().setCreateIfMissing(true)) {
      try (final RocksDB db = RocksDB.open(options, "D:\\playground\\gfsj\\disk")) {

        db.put("foo".getBytes(StandardCharsets.UTF_8), "bar".getBytes(StandardCharsets.UTF_8));
        System.out.println(new String(db.get("foo".getBytes(StandardCharsets.UTF_8))));
      }
    } catch (RocksDBException e) {

    }
  }
}
