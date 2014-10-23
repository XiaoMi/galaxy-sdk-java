package com.xiaomi.infra.galaxy.client.io;

import com.xiaomi.infra.galaxy.api.io.ByteArrayRecordReader;
import com.xiaomi.infra.galaxy.api.io.RecordReader;
import com.xiaomi.infra.galaxy.io.thrift.RSFileHeader;
import com.xiaomi.infra.galaxy.sds.thrift.Datum;
import com.xiaomi.infra.galaxy.sds.thrift.DatumMapMeta;
import com.xiaomi.infra.galaxy.sds.thrift.DatumMapRecord;
import libthrift091.TDeserializer;
import libthrift091.protocol.TCompactProtocol;

import java.util.HashMap;
import java.util.Map;

/**
 * This class is not thread safe
 */
class SDSDatumMapRecordReader implements RecordReader<Map<String, Datum>> {
  private ByteArrayRecordReader reader;
  private Map<Short, String> keyIdMap = new HashMap<Short, String>();

  SDSDatumMapRecordReader(ByteArrayRecordReader reader, DatumMapMeta meta) {
    this.reader = reader;
    if (meta != null && meta.getKeyIdMap() != null) {
      keyIdMap.putAll(meta.getKeyIdMap());
    }
  }

  @Override public RSFileHeader readHeader() throws Exception {
    throw new UnsupportedOperationException("The header must be already read");
  }

  @Override public boolean hasNext() throws Exception {
    return this.reader.hasNext();
  }

  @Override public Map<String, Datum> next() throws Exception {
    byte[] bytes = this.reader.next();
    TDeserializer deserializer = new TDeserializer(new TCompactProtocol.Factory());
    DatumMapRecord dmr = new DatumMapRecord();
    deserializer.deserialize(dmr, bytes);
    if (dmr.isSetKeyIdMap()) {
      keyIdMap.putAll(dmr.getKeyIdMap());
    }
    Map<String, Datum> record = new HashMap<String, Datum>();
    for (Map.Entry<Short, Datum> entry : dmr.getData().entrySet()) {
      String key = keyIdMap.get(entry.getKey());
      if (key == null) {
        throw new IllegalArgumentException("Illegal file, unknown key id: " + entry.getKey());
      }
      record.put(key, entry.getValue());
    }
    return record;
  }
}
