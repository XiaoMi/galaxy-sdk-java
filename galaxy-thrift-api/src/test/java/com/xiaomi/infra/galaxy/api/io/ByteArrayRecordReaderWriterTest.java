package com.xiaomi.infra.galaxy.api.io;

import static org.junit.Assert.assertEquals;

import com.xiaomi.infra.galaxy.io.thrift.Compression;
import com.xiaomi.infra.galaxy.io.thrift.RSFileHeader;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class ByteArrayRecordReaderWriterTest {
  @Test
  public void testSerialization() throws Exception {
    Compression[] compressions = {Compression.NONE, Compression.SNAPPY};
    int[] counts = {0, 1, 10, 100, 1000};
    for (Compression compression : compressions) {
      for (int count : counts) {
        List<String> expected = new ArrayList<String>();
        for (int i = 0; i < count; i++) {
          expected.add("message #" + i);
        }

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        RecordWriter<byte[]> recordWriter = new ByteArrayRecordWriter(outputStream,
            new RSFileHeader().setCompression(compression).setCount(count));
        for (String item : expected) {
          recordWriter.append(item.getBytes("UTF-8"));
        }
        recordWriter.seal();
        outputStream.close();

        InputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        RecordReader<byte[]> recordReader = new ByteArrayRecordReader(inputStream);
        List<String> actual = new ArrayList<String>();
        while (recordReader.hasNext()) {
          actual.add(new String(recordReader.next(), "UTF-8"));
        }
        inputStream.close();
        assertEquals(expected, actual);
      }
    }
  }
}
