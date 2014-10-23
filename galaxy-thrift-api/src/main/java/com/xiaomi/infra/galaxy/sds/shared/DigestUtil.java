package com.xiaomi.infra.galaxy.sds.shared;

import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class DigestUtil {
  public static enum DigestAlgorithm {
    MD5, SHA1, SHA256
  }

  public static byte[] digest(DigestAlgorithm algorithm, byte[] data) {
    try {
      MessageDigest md = MessageDigest.getInstance(algorithm.name());
      return md.digest(data);
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("Failed to calculate digest", e);
    }
  }

  public static byte[] digest(DigestAlgorithm algorithm, InputStream in, long maxSize)
      throws IOException {
    try {
      long allowed = maxSize;
      MessageDigest md = MessageDigest.getInstance(algorithm.name());
      byte[] buffer = new byte[256];
      int read = 0;
      while (read >= 0 && allowed > 0) {
        read = in.read(buffer);
        if (read > 0) {
          allowed -= read;
          md.update(buffer, 0, read);
        }
      }
      if (read >= 0 && allowed <= 0) {
        throw new IllegalArgumentException("Input stream length exceeds limits: " + maxSize);
      }
      return md.digest();

    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("Failed to calculate digest", e);
    }
  }
}
