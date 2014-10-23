package com.xiaomi.infra.galaxy.sds.shared;

import java.io.UnsupportedEncodingException;
import java.security.GeneralSecurityException;
import java.util.List;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

public class SignatureUtil {
  private static final String UTF_8 = "UTF-8";

  public static enum MacAlgorithm {
    HmacMD5, HmacSHA1, HmacSHA256
  }

  private static String multiply(String delim, List<String> parts) {
    assert parts != null;
    if (parts != null) {
      StringBuilder sb = new StringBuilder();
      sb.append(parts.remove(0));
      for (String part : parts) {
        sb.append(delim).append(part);
      }
      return sb.toString();
    }
    return "";
  }

  public static byte[] sign(MacAlgorithm algorithm, String secretKey, List<String> parts) {
    return sign(algorithm, secretKey, multiply("\n", parts));
  }

  public static byte[] sign(MacAlgorithm algorithm, String secretKey, String message) {
    try {
      return sign(algorithm, secretKey.getBytes(UTF_8), message.getBytes(UTF_8));
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException("Unsupported encoding: " + UTF_8, e);
    }
  }

  public static byte[] sign(MacAlgorithm algorithm, byte[] secretKey, byte[] data) {
    try {
      Mac mac = Mac.getInstance(algorithm.name());
      mac.init(new SecretKeySpec(secretKey, algorithm.name()));
      return mac.doFinal(data);
    } catch (GeneralSecurityException e) {
      throw new RuntimeException("MAC algorithm not found: " + algorithm, e);
    }
  }
}
