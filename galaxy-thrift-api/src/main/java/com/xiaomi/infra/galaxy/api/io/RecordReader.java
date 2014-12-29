package com.xiaomi.infra.galaxy.api.io;

import com.xiaomi.infra.galaxy.io.thrift.RSFileHeader;

import java.io.IOException;

public interface RecordReader<R> {
  /**
   * Read file header
   *
   * @return
   * @throws IOException
   */
  RSFileHeader readHeader() throws IOException;

  /**
   * Has next record
   *
   * @return
   * @throws IOException
   */
  boolean hasNext() throws IOException;

  /**
   * Get next record
   *
   * @return
   * @throws IOException
   */
  R next() throws IOException;

  /**
   * Close the underlying stream, don't call this if the underlying stream is self managed.
   *
   * @throws IOException
   */
  void close() throws IOException;
}
