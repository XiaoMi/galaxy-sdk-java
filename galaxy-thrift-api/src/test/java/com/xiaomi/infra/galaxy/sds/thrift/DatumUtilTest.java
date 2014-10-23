package com.xiaomi.infra.galaxy.sds.thrift;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.xiaomi.infra.galaxy.sds.thrift.Datum;
import com.xiaomi.infra.galaxy.sds.thrift.DatumUtil;

public class DatumUtilTest {
  @Test
  public void testConversion() {
    byte[] bytes = { 0x00, 0x01, 0x02, 0x03 };
    Object[] values = { true, false, (byte) -1, (short) 0, (int) 1, Long.MAX_VALUE, "string", 0.1f,
        -0.1, bytes };

    for (Object value : values) {
      Datum datum = DatumUtil.toDatum(value);
      assertEquals(datum, DatumUtil.deserialize(DatumUtil.serialize(datum)));
      assertEquals(datum, DatumUtil.toDatum(DatumUtil.fromDatum(datum)));
    }
  }
}
