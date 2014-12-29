package com.xiaomi.infra.galaxy.client.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.xiaomi.infra.galaxy.api.io.ByteArrayRecordWriter;
import com.xiaomi.infra.galaxy.api.io.RecordWriter;
import com.xiaomi.infra.galaxy.sds.thrift.Datum;
import com.xiaomi.infra.galaxy.sds.thrift.RCBasicMeta;
import com.xiaomi.infra.galaxy.sds.thrift.RCBasicRowGroupHeader;
import com.xiaomi.infra.galaxy.sds.thrift.Value;
import com.xiaomi.infra.galaxy.sds.thrift.ValueList;
import libthrift091.TException;
import libthrift091.TSerializer;
import libthrift091.protocol.TCompactProtocol;

/**
 * This class is not thread safe
 */
class SDSRCBasicRecordWriter implements RecordWriter<Map<String, Datum>> {
  private ByteArrayRecordWriter writer;
  private List<String> keys;
  private List<Map<String, Datum>> groupBuffer;
  private int groupSize;

  public SDSRCBasicRecordWriter(ByteArrayRecordWriter writer, RCBasicMeta meta, int groupSize) {
    this.writer = writer;
    this.keys = meta.getKeys();
    this.groupSize = groupSize;
    this.groupBuffer = new ArrayList<Map<String, Datum>>(groupSize);
    if (this.keys == null || this.keys.isEmpty()) {
      throw new UnsupportedOperationException("Table schema must be defined before write");
    }
  }

  @Override public void append(Map<String, Datum> record) throws IOException {
    if (groupBuffer.size() == groupSize) {
      flush();
    }
    groupBuffer.add(record);
  }

  @Override public void seal() throws IOException {
    flush();
    writer.seal();
  }

  private void flush() throws IOException {
    if (!groupBuffer.isEmpty()) {
      TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
      int numKeys = keys.size();
      List<Integer> offsets = new ArrayList<Integer>(numKeys);
      List<byte[]> buffers = new ArrayList<byte[]>(numKeys);
      int offset = 0;
      for (String key : keys) {
        List<Value> values = new ArrayList<Value>();
        for (Map<String, Datum> row : groupBuffer) {
          Datum datum = row.get(key);
          Value value = datum == null ? Value.nullValue(true) : datum.getValue();
          values.add(value);
        }
        byte[] bytes;
        try {
          bytes = serializer.serialize(new ValueList().setValues(values));
        } catch (TException te) {
          throw new IOException("Failed to serialize column: " + key, te);
        }
        offsets.add(offset);
        offset += bytes.length;
        buffers.add(bytes);
      }
      // write row group header
      RCBasicRowGroupHeader groupHeader = new RCBasicRowGroupHeader()
          .setCount(groupBuffer.size())
          .setOffset(offsets);
      try {
        writer.append(serializer.serialize(groupHeader));
      } catch (TException te) {
        throw new IOException("Failed to serialize row group header", te);
      }
      // write each column list
      for (byte[] bytes : buffers) {
        writer.append(bytes);
      }
      groupBuffer.clear();
    }
  }

  @Override public void close() throws IOException {
    seal();
    writer.close();
  }
}
