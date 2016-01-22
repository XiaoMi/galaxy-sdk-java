/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: xiajun@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.client.compression;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import com.xiaomi.infra.galaxy.talos.thrift.MessageCompressionType;

public class CompressionFactory {
  private static final Logger LOG = LoggerFactory.getLogger(CompressionFactory.class);

  public static DataOutputStream getConpressedOutputStream(MessageCompressionType type, OutputStream outputStream) throws IOException {
    switch (type) {
      case NONE:
        return new DataOutputStream(outputStream);
      case SNAPPY:
        return new DataOutputStream(new SnappyOutputStream(outputStream));
      case GZIP:
        try {
          return new DataOutputStream(new GZIPOutputStream(outputStream));
        } catch (IOException e) {
          LOG.error("Create Gzip OuputStream failed", e);
          throw e;
        }
      default:
        throw new RuntimeException("Unsupported Compression type: " + type);
    }
  }

  public static DataInputStream getDeconpressedInputStream(MessageCompressionType type, ByteBuffer buffer) throws IOException {
    InputStream inputStream = new ByteBufferBackedInputStream(buffer);
    switch (type) {
      case NONE:
        return new DataInputStream(inputStream);
      case SNAPPY:
        try {
          return new DataInputStream(new SnappyInputStream(inputStream));
        } catch (IOException e) {
          LOG.error("Create Snappy InputStream failed", e);
          throw e;
        }
      case GZIP:
        try {
          return new DataInputStream(new GZIPInputStream(inputStream));
        } catch (IOException e) {
          LOG.error("Create Gzip InputStream failed", e);
          throw e;
        }
      default:
        throw new RuntimeException("Unsupported Compression type: " + type);
    }
  }
}
