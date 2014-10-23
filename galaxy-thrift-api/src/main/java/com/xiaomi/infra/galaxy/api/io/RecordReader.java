package com.xiaomi.infra.galaxy.api.io;

import com.xiaomi.infra.galaxy.io.thrift.RSFileHeader;

public interface RecordReader<R> {
  RSFileHeader readHeader() throws Exception;
  boolean hasNext() throws Exception;
  R next() throws Exception;
}
