package com.xiaomi.infra.galaxy.api.io;

import com.xiaomi.infra.galaxy.io.thrift.Compression;
import com.xiaomi.infra.galaxy.io.thrift.RSFileConstants;
import com.xiaomi.infra.galaxy.io.thrift.RSFileHeader;
import com.xiaomi.infra.galaxy.io.thrift.Record;
import libthrift091.protocol.TCompactProtocol;
import libthrift091.protocol.TProtocol;
import libthrift091.transport.TIOStreamTransport;

import java.io.OutputStream;

/**
 * This class is not thread safe
 */
public class ByteArrayRecordWriter implements RecordWriter<byte[]> {
  private static final int VERSION = 1;
  private RSFileHeader header;
  private OutputStream outputStream;
  private TProtocol protocol;
  private boolean headerWritten = false;
  private long count = -1;
  private long counter = 0;

  public ByteArrayRecordWriter(OutputStream outputStream, RSFileHeader header) {
    this.outputStream = outputStream;
    this.header = header.setVersion(VERSION).setMagic(RSFileConstants.MAGIC);
  }

  private boolean tryWriteHeader() throws Exception {
    if (!headerWritten) {
      this.protocol = new TCompactProtocol(new TIOStreamTransport(outputStream));
      this.header.write(this.protocol);
      headerWritten = true;
      Compression compression = header.getCompression();
      outputStream = CompressionStreamAdaptor.getOutputStream(outputStream, compression);
      this.protocol = new TCompactProtocol(new TIOStreamTransport(outputStream));
      return true;
    }
    return false;
  }

  @Override public void append(byte[] record) throws Exception {
    tryWriteHeader();
    Record rec = new Record().setData(record);
    rec.write(this.protocol);
    if (count >= 0) {
      ++counter;
    }
  }

  @Override public void seal() throws Exception {
    tryWriteHeader();
    Record rec = new Record().setEof(true);
    rec.write(this.protocol);
    outputStream.flush(); // flush snappy buffer

    if (count >= 0 && count != counter) {
      throw new IllegalArgumentException(
          String.format("Record count mismatch: %d != %d", counter, count));
    }
  }


}
