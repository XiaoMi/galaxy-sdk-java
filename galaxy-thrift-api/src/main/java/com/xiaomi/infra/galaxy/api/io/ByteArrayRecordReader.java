package com.xiaomi.infra.galaxy.api.io;

import com.xiaomi.infra.galaxy.io.thrift.Compression;
import com.xiaomi.infra.galaxy.io.thrift.RSFileHeader;
import com.xiaomi.infra.galaxy.io.thrift.Record;
import libthrift091.protocol.TCompactProtocol;
import libthrift091.protocol.TProtocol;
import libthrift091.transport.TIOStreamTransport;

import java.io.InputStream;
import java.util.NoSuchElementException;

/**
 * This class is not thread safe
 */
public class ByteArrayRecordReader implements RecordReader<byte[]> {
  private RSFileHeader header;
  private InputStream inputStream;
  private TProtocol protocol;
  private Record next;

  public ByteArrayRecordReader(InputStream inputStream) {
    this.inputStream = inputStream;
    this.header = null;
    this.next = null;
  }

  @Override public RSFileHeader readHeader() throws Exception {
    assert header == null;
    this.protocol = new TCompactProtocol(new TIOStreamTransport(inputStream));
    RSFileHeader fileHeader = new RSFileHeader();
    fileHeader.read(this.protocol);
    this.header = fileHeader;
    Compression compression = header.getCompression();
    InputStream is = CompressionStreamAdaptor.getInputStream(inputStream, compression);
    this.protocol = new TCompactProtocol(new TIOStreamTransport(is));
    return header;
  }

  private void trySkipHeader() throws Exception {
    if (this.header == null) {
      readHeader();
    }
  }

  @Override public boolean hasNext() throws Exception {
    trySkipHeader();
    if (next == null) {
      Record record = new Record();
      record.read(this.protocol);
      this.next = record;
    }
    return !this.next.isEof();
  }

  @Override public byte[] next() throws Exception {
    if (hasNext()) {
      byte[] data = next.getData();
      this.next = null;
      return data;
    } else {
      throw new NoSuchElementException("no more record available");
    }
  }
}
