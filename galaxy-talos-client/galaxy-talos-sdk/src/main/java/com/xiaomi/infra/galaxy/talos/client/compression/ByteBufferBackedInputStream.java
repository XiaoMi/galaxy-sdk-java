/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: xiajun@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.client.compression;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class ByteBufferBackedInputStream extends InputStream{
  private ByteBuffer buffer;
  public ByteBufferBackedInputStream(ByteBuffer byteBuffer) {
    this.buffer = byteBuffer;
  }

  @Override
  public int read() throws IOException {
    if (buffer.hasRemaining()) {
      return buffer.get() & 0xFF;
    } else {
      return -1;
    }
  }

  @Override
  public int read(byte[] bytes, int off, int len) throws IOException {
    if (buffer.hasRemaining()) {
      int readLen = Math.min(len, buffer.remaining());
      buffer.get(bytes, off, readLen);
      return readLen;
    } else {
      return -1;
    }
  }
}
