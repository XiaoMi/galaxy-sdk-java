package com.xiaomi.infra.galaxy.sds.shared;

import java.math.BigInteger;

import org.apache.commons.codec.binary.Base64;

public class BytesUtil {

  public static BigInteger bytesToBigInteger(byte[] data) {
    return new BigInteger(1, data);
  }

  public static BigInteger hexToBigInteger(String hex) {
    return new BigInteger(hex, 16);
  }

  public static String bytesToHex(byte[] data) {
    BigInteger bigInt = new BigInteger(1, data);
    return bigInt.toString(16);
  }

  public static String bytesToBase64(byte[] data) {
    return new String(Base64.encodeBase64(data));
  }
}
