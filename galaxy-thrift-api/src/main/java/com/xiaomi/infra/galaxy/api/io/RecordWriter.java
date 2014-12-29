package com.xiaomi.infra.galaxy.api.io;

import java.io.IOException;

public interface RecordWriter<R> {
  /**
   * Append a record to the underlying stream
   *
   * @param record
   * @throws IOException
   */
  void append(R record) throws IOException;

  /**
   * Seal and no further record can be written.
   *
   * @throws IOException
   */
  void seal() throws IOException;

  /**
   * Close the underlying stream, don't call this if the underlying stream is self managed.
   *
   * @throws IOException
   */
  void close() throws IOException;
}
