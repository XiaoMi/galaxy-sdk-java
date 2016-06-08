package com.xiaomi.infra.galaxy.sds.client.examples.stream;

import com.google.common.collect.Lists;
import com.xiaomi.infra.galaxy.sds.thrift.CannedAcl;
import com.xiaomi.infra.galaxy.sds.thrift.DataType;
import com.xiaomi.infra.galaxy.sds.thrift.Datum;
import com.xiaomi.infra.galaxy.sds.thrift.DatumUtil;
import com.xiaomi.infra.galaxy.sds.thrift.EntityGroupSpec;
import com.xiaomi.infra.galaxy.sds.thrift.KeySpec;
import com.xiaomi.infra.galaxy.sds.thrift.ProvisionThroughput;
import com.xiaomi.infra.galaxy.sds.thrift.StreamSpec;
import com.xiaomi.infra.galaxy.sds.thrift.TableMetadata;
import com.xiaomi.infra.galaxy.sds.thrift.TableQuota;
import com.xiaomi.infra.galaxy.sds.thrift.TableSchema;
import com.xiaomi.infra.galaxy.sds.thrift.TableSpec;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

public class DataProvider {
  static final String ENTITY_GROUP_KEY = "entity_group_key";
  static final List<KeySpec> ENTITY_GROUP_KEYS =
      Arrays.asList(new KeySpec[] { new KeySpec().setAttribute(ENTITY_GROUP_KEY).setAsc(true) });

  static final String PRIMARY_KEY1 = "imei";
  static final String PRIMARY_KEY2 = "timestamp";
  static final List<KeySpec> PRIMARY_KEYS =
      Arrays.asList(new KeySpec[] { new KeySpec().setAttribute(PRIMARY_KEY1).setAsc(true),
        new KeySpec().setAttribute(PRIMARY_KEY2).setAsc(true) });

  static Random rand = new Random();
  static final int preSplits = 32;
  static final int ttl = -1;
  static final long tableSize = 1024 * 1024 * 1024;
  static final long readCapacity = 5000;
  static final long writeCapacity = 5000;
  static final long exceededReadCapacity = 10000;
  static final long exceededWriteCapacity = 10000;

  public static Map<String, List<CannedAcl>> cannedAclGrant(String appId, CannedAcl... cannedAcls) {
    Map<String, List<CannedAcl>> appGrant = new HashMap<String, List<CannedAcl>>();
    appGrant.put(appId, Arrays.asList(cannedAcls));
    return appGrant;
  }

  public static TableSpec createTableSpec(String appId, boolean enableEntityGroup,
      boolean enableEntityGroupHash) {
    return createTableSpec(appId, enableEntityGroup, enableEntityGroupHash, null);
  }

  public static TableSpec createTableSpec(String appId, boolean enableEntityGroup,
      boolean enableEntityGroupHash, Map<String, StreamSpec> streamSpecs) {
    EntityGroupSpec groupSpec = enableEntityGroup ?
        new EntityGroupSpec().setAttributes(ENTITY_GROUP_KEYS)
            .setEnableHash(enableEntityGroupHash) : null;
    TableSchema tableSchema = new TableSchema();
    tableSchema.setEntityGroup(groupSpec)
        .setPrimaryIndex(PRIMARY_KEYS)
        .setAttributes(DataProvider.attributesDef(enableEntityGroup))
        .setPreSplits(preSplits)
        .setTtl(ttl);

    TableMetadata tableMetadata = new TableMetadata();
    tableMetadata.setAppAcl(cannedAclGrant(appId, CannedAcl.values()))
        .setQuota(new TableQuota().setSize(tableSize))
        .setAppAcl(cannedAclGrant(appId, CannedAcl.APP_SECRET_READ, CannedAcl.APP_SECRET_WRITE))
        .setThroughput(new ProvisionThroughput()
            .setReadCapacity(readCapacity)
            .setWriteCapacity(writeCapacity))
        .setExceededThroughput(new ProvisionThroughput()
            .setReadCapacity(exceededReadCapacity)
            .setWriteCapacity(exceededWriteCapacity));
    if (streamSpecs != null) {
      tableSchema.setStreams(streamSpecs);
    }

    return new TableSpec().setSchema(tableSchema).setMetadata(tableMetadata);
  }

  public static Map<String, DataType> rowKeyDef(boolean enableEntityGroup) {
    Map<String, DataType> attrDef = new HashMap<String, DataType>();
    if (enableEntityGroup) {
      attrDef.put(ENTITY_GROUP_KEY, DataType.STRING);
    }
    attrDef.put(PRIMARY_KEY1, DataType.STRING);
    attrDef.put(PRIMARY_KEY2, DataType.INT64);
    return attrDef;
  }

  public static Map<String, DataType> attributesDef(boolean enableEntityGroup) {
    Map<String, DataType> attrDef = new HashMap<String, DataType>();
    attrDef.putAll(rowKeyDef(enableEntityGroup));
    attrDef.putAll(columnsDef());
    return attrDef;
  }

  public static Map<String, DataType> columnsDef() {
    Map<String, DataType> attrDef = new HashMap<String, DataType>();
    for (DataType type : DataType.values()) {
      if (type.equals(DataType.BINARY_SET) ||
          type.equals(DataType.BOOL_SET) ||
          type.equals(DataType.DOUBLE_SET) ||
          type.equals(DataType.FLOAT_SET) ||
          type.equals(DataType.INT16_SET) ||
          type.equals(DataType.INT32_SET) ||
          type.equals(DataType.INT64_SET) ||
          type.equals(DataType.INT8_SET) ||
          type.equals(DataType.STRING_SET))
        continue;

      attrDef.put(columnName(type), type);
    }
    return attrDef;
  }

  public static String columnName(DataType type) {
    return type.name();
  }

  public static Map<String, Datum> getRecordKeys(TableSchema tableSchema,
      Map<String, Datum> record) {
    Set<String> rowKey = attributes(tableSchema.getPrimaryIndex());
    if (tableSchema.getEntityGroup() != null) {
      rowKey.addAll(attributes(tableSchema.getEntityGroup().getAttributes()));
    }
    return DataProvider.filter(record, rowKey);
  }

  static Set<String> attributes(Collection<KeySpec> keySpecs) {
    Set<String> attrs = new HashSet<String>();
    if (keySpecs != null) {
      for (KeySpec keySpec : keySpecs) {
        attrs.add(keySpec.getAttribute());
      }
    }
    return attrs;
  }

  public static Map<String, Datum> filter(Map<String, Datum> keyValues, Set<String> targetKeys) {
    if (keyValues != null) {
      Map<String, Datum> kvs = new HashMap<String, Datum>();
      for (Map.Entry<String, Datum> e : keyValues.entrySet()) {
        if (targetKeys.contains(e.getKey())) {
          kvs.put(e.getKey(), e.getValue());
        }
      }
      return kvs;
    }
    return null;
  }


  public static Map<String, Datum> randomRecord(Map<String, DataType> attrDefs) {
    Map<String, Datum> kvs = new HashMap<String, Datum>();
    for (Map.Entry<String, DataType> e : attrDefs.entrySet()) {
      if (ENTITY_GROUP_KEY.equals(e.getKey())) {
        kvs.put(e.getKey(), rand(1, e.getValue()));
      } else {
        kvs.put(e.getKey(), rand(0, e.getValue()));
      }
    }
    return kvs;
  }

  public static Datum rand(int minLength, DataType type) {
    Object value = null;
    int len = Math.max(minLength, rand.nextInt(50));
    int N = 5;
    switch (type) {
    case BOOL:
      value = rand.nextBoolean();
      return DatumUtil.toDatum(value);
    case INT8:
      value = (byte) rand.nextInt();
      return DatumUtil.toDatum(value);
    case INT16:
      value = (short) rand.nextInt();
      return DatumUtil.toDatum(value);
    case INT32:
      value = rand.nextInt();
      return DatumUtil.toDatum(value);
    case INT64:
      value = rand.nextLong();
      return DatumUtil.toDatum(value);
    case FLOAT:
      value = rand.nextFloat();
      return DatumUtil.toDatum(value);
    case DOUBLE:
      value = rand.nextDouble();
      return DatumUtil.toDatum(value);
    case STRING:
      if (len == 0) {
        value = "";
      } else {
        value = new BigInteger(len * 8, rand).toString(Character.MAX_RADIX);
      }
      return DatumUtil.toDatum(value);
    case BINARY:
    case RAWBINARY:
      byte[] bytes = new byte[len];
      rand.nextBytes(bytes);
      value = bytes;
      return DatumUtil.toDatum(value);
    default:
      throw new RuntimeException("Invalid type " + type);
    }
  }

  public static List<String> randomSelect(Set<String> attributes, int n) {
    if (n >= attributes.size()) {
      return Lists.newArrayList(attributes);
    }
    Random random = new Random();
    List<String> attributesList = new ArrayList<String>(n);
    int k = 0;
    for (String attribute : attributes) {
      if (k < n) {
        attributesList.add(attribute);
      } else {
        int i = random.nextInt(k + 1);
        if (i < n) {
          attributesList.set(i, attribute);
        }
      }
      k++;
    }
    return attributesList;
  }

  public static Map<String, Datum> randomIncrementAmounts(TableSchema tableSchema,
      Set<String> attributes) {
    Map<String, Datum> amounts = new HashMap<String, Datum>();
    Map<String, DataType> attrDefs = tableSchema.getAttributes();
    for (String e : attributes) {
      DataType dataType = attrDefs.get(e);
        if (dataType.equals(DataType.INT32)) {
          amounts.put(e, DatumUtil.toDatum(rand.nextInt(10)));
        } else if (dataType.equals(DataType.INT64)) {
          amounts.put(e, DatumUtil.toDatum(Math.min(rand.nextLong(), 10)));
        }
    }
    return amounts;
  }
}
