package com.xiaomi.infra.galaxy.client.io;

import com.xiaomi.infra.galaxy.api.io.RecordReader;
import com.xiaomi.infra.galaxy.api.io.RecordWriter;
import com.xiaomi.infra.galaxy.io.thrift.Compression;
import com.xiaomi.infra.galaxy.sds.thrift.DataType;
import com.xiaomi.infra.galaxy.sds.thrift.Datum;
import com.xiaomi.infra.galaxy.sds.thrift.DatumUtil;
import com.xiaomi.infra.galaxy.sds.thrift.SLFileType;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class SDSRecordReaderWriterTest {
  private static String[] cities = { "北京", "Beihai", "Dalian", "Dandong", "Fuzhou", "Guangzhou",
      "Haikou", "Hankou", "Huangpu", "Jiujiang", "Lianyungang", "Nanjing", "Nantong", "Ningbo",
      "Qingdao", "Qinhuangdao", "Rizhao", "Sanya", "Shanghai", "Shantou", "Shenzhen",
      "Tianjin", "Weihai", "Wenzhou", "Xiamen", "Yangzhou", "Yantai" };

  @Test
  public void testSerialization() throws Exception {
    Map<String, DataType> typeMap = new HashMap<String, DataType>();
    typeMap.put("cityId", DataType.STRING);
    typeMap.put("timestamp", DataType.INT64);
    typeMap.put("score", DataType.DOUBLE);
    typeMap.put("pm25", DataType.INT64);

    List<Map<String, DataType>> typesList = new ArrayList<Map<String, DataType>>();
    typesList.add(typeMap);
    typesList.add(null);
    Compression[] compressions = { Compression.NONE, Compression.SNAPPY };
    int[] counts = { 0, 1, 10000 };
    SLFileType[] fileTypes = { SLFileType.DATUM_MAP, SLFileType.RC_BASIC };

    for (Map<String, DataType> types : typesList) {
      for (Compression compression : compressions) {
        for (SLFileType fileType : fileTypes) {
          for (int count : counts) {
            Random rand = new Random(0);
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

            RecordWriter<Map<String, Datum>> recordWriter = null;
            try {
              recordWriter = SDSRecordReaderWriterFactory.getRecordWriter(outputStream, types,
                  fileType, compression);
            } catch (UnsupportedOperationException uoe) {
              System.out.println(fileType + "." + compression + ":\tunsupported");
              continue;
            }

            List<Map<String, Datum>> records = new ArrayList<Map<String, Datum>>();
            for (int i = 0; i < count; i++) {
              Date now = new Date();
              Map<String, Datum> record = new HashMap<String, Datum>();
              record.put("cityId", DatumUtil.toDatum(cities[i % cities.length]));
              record.put("timestamp", DatumUtil.toDatum(now.getTime()));
              record.put("score", DatumUtil.toDatum((double) rand.nextInt(100)));
              if (rand.nextBoolean()) { // test null value
                record.put("pm25", DatumUtil.toDatum((long) rand.nextInt(500)));
              }
              recordWriter.append(record);
              records.add(record);
            }
            recordWriter.seal();
            System.out.println(fileType + "." + compression + " file size:\t"
                + outputStream.size());
            recordWriter.close();

            InputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
            RecordReader<Map<String, Datum>> recordReader =
                SDSRecordReaderWriterFactory.getRecordReader(inputStream);
            List<Map<String, Datum>> actual = new ArrayList<Map<String, Datum>>();
            while (recordReader.hasNext()) {
              actual.add(recordReader.next());
            }
            recordReader.close();

            assertEquals(records, actual);
          }
        }
      }
    }
  }
}
