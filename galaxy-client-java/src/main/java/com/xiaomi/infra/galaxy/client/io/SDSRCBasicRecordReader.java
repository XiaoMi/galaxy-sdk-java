package com.xiaomi.infra.galaxy.client.io;

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
import libthrift091.protocol.TCompactProtocol;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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

  @Override public RSFileHeader readHeader() throws Exception {
    throw new UnsupportedOperationException("The header must be already read");
  }

  @Override public boolean hasNext() throws Exception {
    if (groupBufferIterator != null && groupBufferIterator.hasNext()) {
      return true;
    }
    return reader.hasNext();
  }

  @Override public Map<String, Datum> next() throws Exception {
    if (groupBufferIterator == null || !groupBufferIterator.hasNext()) {
      TDeserializer deserializer = new TDeserializer(new TCompactProtocol.Factory());
      byte[] headerBytes = this.reader.next(); // read group header
      RCBasicRowGroupHeader groupHeader = new RCBasicRowGroupHeader();
      deserializer.deserialize(groupHeader, headerBytes);

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
        byte[] bytes = this.reader.next();
        ValueList columnList = new ValueList();
        deserializer.deserialize(columnList, bytes);
        assert columnList.getValuesSize() == groupHeader.getCount();
        String key = keys.get(kid);
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
}
