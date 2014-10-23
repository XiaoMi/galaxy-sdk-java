package com.xiaomi.infra.galaxy.api.io;

public interface RecordWriter<R> {
  void append(R record) throws Exception;
  void seal() throws Exception;
}
