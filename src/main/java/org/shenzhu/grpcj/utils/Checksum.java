package org.shenzhu.grpcj.utils;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Checksum {
  public static byte[] getCheckSum(byte[] data) throws NoSuchAlgorithmException {
    MessageDigest md = MessageDigest.getInstance("MD5");
    md.update(data);

    return md.digest();
  }
}
