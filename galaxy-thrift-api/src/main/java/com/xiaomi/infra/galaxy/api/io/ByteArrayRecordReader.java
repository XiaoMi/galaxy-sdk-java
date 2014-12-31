package com.xiaomi.infra.galaxy.api.io;

import com.xiaomi.infra.galaxy.io.thrift.Compression;
import com.xiaomi.infra.galaxy.io.thrift.RSFileHeader;
import com.xiaomi.infra.galaxy.io.thrift.Record;
import libthrift091.TException;
import libthrift091.protocol.TCompactProtocol;
import libthrift091.protocol.TProtocol;
import libthrift091.transport.TIOStreamTransport;

import java.io.IOException;
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

  @Override public RSFileHeader readHeader() throws IOException {
    assert header == null;
    this.protocol = new TCompactProtocol(new TIOStreamTransport(inputStream));
    RSFileHeader fileHeader = new RSFileHeader();
    try {
      fileHeader.read(this.protocol);
    } catch (TException te) {
      throw new IOException("Failed to read file header", te);
    }
    this.header = fileHeader;
    Compression compression = header.getCompression();
    InputStream is = CompressionStreamAdaptor.getInputStream(inputStream, compression);
    this.protocol = new TCompactProtocol(new TIOStreamTransport(is));
    return header;
  }

  private void trySkipHeader() throws IOException {
    if (this.header == null) {
      readHeader();
    }
  }

  @Override public boolean hasNext() throws IOException {
    trySkipHeader();
    if (next == null) {
      Record record = new Record();
      try {
        record.read(this.protocol);
      } catch (TException te) {
        throw new IOException("Failed to read record", te);
      }
      this.next = record;
    }
    return !this.next.isEof();
  }

  @Override public byte[] next() throws IOException {
    if (hasNext()) {
      byte[] data = next.getData();
      this.next = null;
      return data;
    } else {
      throw new NoSuchElementException("no more record available");
    }
  }

  @Override public void close() throws IOException {
    this.inputStream.close();
  }
}
