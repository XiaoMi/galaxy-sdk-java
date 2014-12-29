package com.xiaomi.infra.galaxy.client.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.xiaomi.infra.galaxy.api.io.ByteArrayRecordReader;
import com.xiaomi.infra.galaxy.api.io.RecordReader;
import com.xiaomi.infra.galaxy.io.thrift.RSFileHeader;
import com.xiaomi.infra.galaxy.sds.thrift.DataType;
import com.xiaomi.infra.galaxy.sds.thrift.Datum;
import com.xiaomi.infra.galaxy.sds.thrift.RCBasicMeta;
import com.xiaomi.infra.galaxy.sds.thrift.RCBasicRowGroupHeader;
import com.xiaomi.infra.galaxy.sds.thrift.Value;
import com.xiaomi.infra.galaxy.sds.thrift.ValueList;
import libthrift091.TDeserializer;
import libthrift091.TException;
import libthrift091.protocol.TCompactProtocol;

/**
 * This class is not thread safe
 */
class SDSRCBasicRecordReader implements RecordReader<Map<String, Datum>> {
  private ByteArrayRecordReader reader;
  private RCBasicMeta meta;
  private Iterator<Map<String, Datum>> groupBufferIterator;

  public SDSRCBasicRecordReader(ByteArrayRecordReader reader,
                                RCBasicMeta meta) {
    this.reader = reader;
    this.meta = meta;
  }

  @Override public RSFileHeader readHeader() throws IOException {
    throw new UnsupportedOperationException("The header must be already read");
  }

  @Override public boolean hasNext() throws IOException {
    if (groupBufferIterator != null && groupBufferIterator.hasNext()) {
      return true;
    }
    return reader.hasNext();
  }

  @Override public Map<String, Datum> next() throws IOException {
    if (groupBufferIterator == null || !groupBufferIterator.hasNext()) {
      TDeserializer deserializer = new TDeserializer(new TCompactProtocol.Factory());
      byte[] headerBytes = this.reader.next(); // read group header
      RCBasicRowGroupHeader groupHeader = new RCBasicRowGroupHeader();
      try {
        deserializer.deserialize(groupHeader, headerBytes);
      } catch (TException te) {
        throw new IOException("Failed to parse row group header", te);
      }

      int numKeys = meta.getKeys().size();
      List<String> keys = meta.getKeys();
      Map<String, DataType> dataTypes = meta.getTypes();
      List<Map<String, Datum>> groupBuffer =
          new ArrayList<Map<String, Datum>>(groupHeader.getCount());
      for (int i = 0; i < groupHeader.getCount(); ++i) {
        groupBuffer.add(new HashMap<String, Datum>());
      }

      // read each column list
      for (int kid = 0; kid < numKeys; ++kid) {
        String key = keys.get(kid);
        byte[] bytes = this.reader.next();
        ValueList columnList = new ValueList();
        try {
          deserializer.deserialize(columnList, bytes);
        } catch (TException te) {
          throw new IOException("Failed to parse column: " + key, te);
        }
        assert columnList.getValuesSize() == groupHeader.getCount();
        DataType dataType = dataTypes.get(key);
        for (int i = 0; i < columnList.getValuesSize(); ++i) {
          Value value = columnList.getValues().get(i);
          if (!value.isSetNullValue() || !value.getNullValue()) {
            groupBuffer.get(i).put(key, new Datum().setValue(value).setType(dataType));
          } // else skip null value
        }
      }

      groupBufferIterator = groupBuffer.iterator();
    }

    return groupBufferIterator.next();
  }

  @Override public void close() throws IOException {
    reader.close();
  }
}
