package com.xiaomi.infra.galaxy.client.io;

import com.xiaomi.infra.galaxy.api.io.ByteArrayRecordReader;
import com.xiaomi.infra.galaxy.api.io.ByteArrayRecordWriter;
import com.xiaomi.infra.galaxy.api.io.RecordReader;
import com.xiaomi.infra.galaxy.api.io.RecordWriter;
import com.xiaomi.infra.galaxy.io.thrift.Compression;
import com.xiaomi.infra.galaxy.io.thrift.RSFileHeader;
import com.xiaomi.infra.galaxy.sds.thrift.DataType;
import com.xiaomi.infra.galaxy.sds.thrift.Datum;
import com.xiaomi.infra.galaxy.sds.thrift.DatumMapMeta;
import com.xiaomi.infra.galaxy.sds.thrift.RCBasicMeta;
import com.xiaomi.infra.galaxy.sds.thrift.SLFileMeta;
import com.xiaomi.infra.galaxy.sds.thrift.SLFileType;
import libthrift091.TDeserializer;
import libthrift091.TException;
import libthrift091.TSerializer;
import libthrift091.protocol.TCompactProtocol;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SDSRecordReaderWriterFactory {
  public static RecordReader<Map<String, Datum>> getRecordReader(InputStream inputStream)
      throws Exception {
    ByteArrayRecordReader recordReader = new ByteArrayRecordReader(inputStream);
    RSFileHeader header = recordReader.readHeader();
    if (header.getMetadata() != null) {
      SLFileMeta metadata = new SLFileMeta();
      TDeserializer deserializer = new TDeserializer(new TCompactProtocol.Factory());
      deserializer.deserialize(metadata, header.getMetadata());
      switch (metadata.getType()) {
      case DATUM_MAP:
        return new SDSDatumMapRecordReader(recordReader, metadata.getDatumMapMeta());
      case RC_BASIC:
        return new SDSRCBasicRecordReader(recordReader, metadata.getRcBasicMeta());
      default:
        throw new IllegalArgumentException("Unsupported file type: " + metadata.getType());
      }
    }

    throw new IllegalArgumentException("Invalid file format, not sds metadata found");
  }

  public static RecordWriter<Map<String, Datum>> getDatumMapRecordWriter(OutputStream outputStream,
      Map<String, DataType> dataTypes, Compression compression) throws TException {
    TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
    Map<Short, String> keyIdMap = new HashMap<Short, String>();
    short i = 0;
    if (dataTypes != null) {
      for (String key : dataTypes.keySet()) {
        keyIdMap.put(i++, key);
      }
    }

    SLFileMeta metadata = new SLFileMeta()
        .setType(SLFileType.DATUM_MAP)
        .setDatumMapMeta(new DatumMapMeta().setKeyIdMap(keyIdMap));
    byte[] metaBytes = serializer.serialize(metadata);
    RSFileHeader header = new RSFileHeader()
        .setCompression(compression)
        .setMetadata(metaBytes);
    ByteArrayRecordWriter recordWriter = new ByteArrayRecordWriter(outputStream, header);
    return new SDSDatumMapRecordWriter(recordWriter, metadata.getDatumMapMeta());

  }

  public static RecordWriter<Map<String, Datum>> getRCBasicRecordWriter(OutputStream outputStream,
      Map<String, DataType> dataTypes, int rowGroupSize, Compression compression) throws TException {
    TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
    List<String> keys = null;
    if (dataTypes != null) {
      keys = new ArrayList<String>();
      for (Map.Entry<String, DataType> entry : dataTypes.entrySet()) {
        keys.add(entry.getKey());
      }
    }

    SLFileMeta metadata = new SLFileMeta()
        .setType(SLFileType.RC_BASIC)
        .setRcBasicMeta(new RCBasicMeta().setKeys(keys).setTypes(dataTypes));
    byte[] metaBytes = serializer.serialize(metadata);
    RSFileHeader header = new RSFileHeader()
        .setCompression(compression)
        .setMetadata(metaBytes);
    ByteArrayRecordWriter recordWriter = new ByteArrayRecordWriter(outputStream, header);
    return new SDSRCBasicRecordWriter(recordWriter, metadata.getRcBasicMeta(), rowGroupSize);
  }

  public static RecordWriter<Map<String, Datum>> getRecordWriter(OutputStream outputStream,
      Map<String, DataType> dataTypes, SLFileType fileType, Compression compression)
      throws TException {
    switch (fileType) {
    case DATUM_MAP:
      return getDatumMapRecordWriter(outputStream, dataTypes, compression);
    case RC_BASIC:
      return getRCBasicRecordWriter(outputStream, dataTypes, 1000, compression);
    default:
      throw new IllegalArgumentException("Unsupported file type: " + fileType);
    }
  }
}
