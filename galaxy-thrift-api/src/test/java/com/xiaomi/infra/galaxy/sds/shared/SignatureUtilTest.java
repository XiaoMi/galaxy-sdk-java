package com.xiaomi.infra.galaxy.sds.shared;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import org.junit.Test;

import com.xiaomi.infra.galaxy.sds.shared.SignatureUtil.MacAlgorithm;

public class SignatureUtilTest {
  @Test
  public void testSignature() throws Exception {
    String secretKey = "secret key";
    String message = "target string to sign";

    for (MacAlgorithm hash : MacAlgorithm.values()) {
      String expected = BytesUtil.bytesToHex(SignatureUtil.sign(hash, secretKey, message));
      String actual = BytesUtil.bytesToHex(SignatureUtil.sign(hash, secretKey, message));
      assertEquals(expected, actual);
      actual = BytesUtil.bytesToHex(SignatureUtil.sign(hash, secretKey, message + "1"));
      assertNotEquals(expected, actual);
    }
  }
}
