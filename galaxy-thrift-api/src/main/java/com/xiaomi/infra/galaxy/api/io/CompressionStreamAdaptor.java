package com.xiaomi.infra.galaxy.api.io;

import com.xiaomi.infra.galaxy.io.thrift.Compression;
import org.xerial.snappy.SnappyFramedInputStream;
import org.xerial.snappy.SnappyFramedOutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class CompressionStreamAdaptor {
  public static InputStream getInputStream(InputStream inputStream, Compression compression)
      throws IOException {
    if (compression != null) {
      switch (compression) {
      case SNAPPY:
        return new SnappyFramedInputStream(inputStream);
      case NONE:
        return inputStream;
      default:
        throw new IllegalArgumentException("Unsupported compression codec: " + compression);
      }
    }
    return inputStream;
  }

  public static OutputStream getOutputStream(OutputStream outputStream, Compression compression)
      throws IOException {
    if (compression != null) {
      switch (compression) {
      case SNAPPY:
        return new SnappyFramedOutputStream(outputStream);
      case NONE:
        return outputStream;
      default:
        throw new IllegalArgumentException("Unsupported compression codec: " + compression);
      }
    }
    return outputStream;
  }
}
