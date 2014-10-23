package com.xiaomi.infra.galaxy.client.io;

import com.xiaomi.infra.galaxy.api.io.ByteArrayRecordWriter;
import com.xiaomi.infra.galaxy.api.io.RecordWriter;
import com.xiaomi.infra.galaxy.sds.thrift.Datum;
import com.xiaomi.infra.galaxy.sds.thrift.DatumMapMeta;
import com.xiaomi.infra.galaxy.sds.thrift.DatumMapRecord;
import libthrift091.TSerializer;
import libthrift091.protocol.TCompactProtocol;

import java.util.HashMap;
import java.util.Map;

/**
 * This class is not thread safe
 */
class SDSDatumMapRecordWriter implements RecordWriter<Map<String, Datum>> {
  private ByteArrayRecordWriter writer;
  private Map<String, Short> keyIdLookupTable = new HashMap<String, Short>();;
  private TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());

  SDSDatumMapRecordWriter(ByteArrayRecordWriter writer, DatumMapMeta metadata) {
    this.writer = writer;
    if (metadata.getKeyIdMap() != null) {
      for (Map.Entry<Short, String> entry : metadata.getKeyIdMap().entrySet()) {
        keyIdLookupTable.put(entry.getValue(), entry.getKey());
      }
    }
  }

  @Override public void append(Map<String, Datum> record) throws Exception {
    Map<Short, Datum> rec = new HashMap<Short, Datum>();
    boolean containsAll = true;
    short maxKeyId = -1;
    for (Map.Entry<String, Datum> e : record.entrySet()) {
      Short keyId = keyIdLookupTable.get(e.getKey());
      if (keyId == null) {
        containsAll = false;
      } else {
        if (maxKeyId < keyId) {
          maxKeyId = keyId;
        }
        rec.put(keyId, e.getValue());
      }
    }

    DatumMapRecord dmr = new DatumMapRecord();
    if (!containsAll) {
      Map<Short, String> newKeyIdMap = new HashMap<Short, String>();
      for (Map.Entry<String, Datum> e : record.entrySet()) {
        Short keyId = keyIdLookupTable.get(e.getKey());
        if (keyId == null) {
          keyId = ++maxKeyId;
          newKeyIdMap.put(keyId, e.getKey());
          rec.put(keyId, e.getValue());
        }
      }
      dmr.setKeyIdMap(newKeyIdMap);
    }

    byte[] bytes = serializer.serialize(dmr.setData(rec));
    writer.append(bytes);
  }

  @Override public void seal() throws Exception {
    writer.seal();
  }
}
