package com.xiaomi.infra.galaxy.vision.client;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import com.xiaomi.infra.galaxy.vision.model.Image;

public class IOUtils {
  private static final int STREAM_BUFFER_SIZE = 4096;
  
  public static byte[] toByteArray(InputStream in) throws IOException {
    byte[] buffer = new byte[STREAM_BUFFER_SIZE];
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    int len = 0;
    while ((len = in.read(buffer)) > 0) {
      out.write(buffer, 0, len);
    }
    return out.toByteArray();
  }
  
  public static byte[] loadImage(String filePath) throws IOException {
    Image.checkImageFormat(filePath);
    FileInputStream in = null;
    try {
      in = new FileInputStream(filePath);
      return toByteArray(in);
    } finally {
      if (in != null) {
        in.close();
      }
    }
  }
}
