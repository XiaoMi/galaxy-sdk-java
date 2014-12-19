package com.xiaomi.infra.galaxy.sds.thrift;

import libthrift091.TDeserializer;
import libthrift091.TException;
import libthrift091.TSerializer;
import libthrift091.protocol.TCompactProtocol;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static com.xiaomi.infra.galaxy.sds.thrift.Value.binarySetValue;
import static com.xiaomi.infra.galaxy.sds.thrift.Value.binaryValue;
import static com.xiaomi.infra.galaxy.sds.thrift.Value.boolSetValue;
import static com.xiaomi.infra.galaxy.sds.thrift.Value.boolValue;
import static com.xiaomi.infra.galaxy.sds.thrift.Value.doubleSetValue;
import static com.xiaomi.infra.galaxy.sds.thrift.Value.doubleValue;
import static com.xiaomi.infra.galaxy.sds.thrift.Value.int16SetValue;
import static com.xiaomi.infra.galaxy.sds.thrift.Value.int16Value;
import static com.xiaomi.infra.galaxy.sds.thrift.Value.int32SetValue;
import static com.xiaomi.infra.galaxy.sds.thrift.Value.int32Value;
import static com.xiaomi.infra.galaxy.sds.thrift.Value.int64SetValue;
import static com.xiaomi.infra.galaxy.sds.thrift.Value.int64Value;
import static com.xiaomi.infra.galaxy.sds.thrift.Value.int8SetValue;
import static com.xiaomi.infra.galaxy.sds.thrift.Value.int8Value;
import static com.xiaomi.infra.galaxy.sds.thrift.Value.stringSetValue;
import static com.xiaomi.infra.galaxy.sds.thrift.Value.stringValue;

public class DatumUtil {
  public static <T> Map<String, Datum> toDatum(Map<String, T> keyValues) {
    return toDatum(keyValues, (Map<String, DataType>) null);
  }

  public static <T> Map<String, Datum> toDatum(Map<String, T> keyValues,
      Map<String, DataType> attributes) {
    if (keyValues != null) {
      try {
        Map<String, Datum> kvs = new HashMap<String, Datum>();
        for (Map.Entry<String, T> kv : keyValues.entrySet()) {
          if (attributes != null && kv.getValue() instanceof Set) {
            kvs.put(kv.getKey(),
                fromSetToDatum((Set) (kv.getValue()), attributes.get(kv.getKey())));
          } else {
            kvs.put(kv.getKey(), DatumUtil.toDatum(kv.getValue()));
          }
        }
        return kvs;
      } catch (IllegalArgumentException iae) {
        throw new IllegalArgumentException("Failed to convert key values [" + keyValues + "]: "
            + iae.getMessage(), iae);
      }
    }
    return null;
  }

  private static <T> Datum fromSetToDatum(Set<T> set, DataType dataType) {
    List<T> list = new ArrayList<T>();
    for (T e : set) {
      list.add(e);
    }
    switch (dataType) {
    case BOOL_SET:
      return toDatum(list, DataType.BOOL);
    case INT8_SET:
      return toDatum(list, DataType.INT8);
    case INT16_SET:
      return toDatum(list, DataType.INT16);
    case INT32_SET:
      return toDatum(list, DataType.INT32);
    case INT64_SET:
      return toDatum(list, DataType.INT64);
    case FLOAT_SET:
      return toDatum(list, DataType.FLOAT);
    case DOUBLE_SET:
      return toDatum(list, DataType.DOUBLE);
    case STRING_SET:
      return toDatum(list, DataType.STRING);
    case BINARY_SET:
      return toDatum(list, DataType.BINARY);
    default:
      throw new RuntimeException("Unsupported repeated type" + dataType);
    }
  }

  public static Datum toDatum(Object value) {
    return toDatum(value, null);
  }

  public static Datum toDatum(Object value, DataType repeatedType) {
    if (value == null) {
      throw new IllegalArgumentException("Datum must not be null");
    }
    if (repeatedType == null) {
      if (value instanceof Boolean) {
        return newDatum(DataType.BOOL, boolValue((Boolean) value));
      } else if (value instanceof Byte) {
        return newDatum(DataType.INT8, int8Value((Byte) value));
      } else if (value instanceof Short) {
        return newDatum(DataType.INT16, int16Value((Short) value));
      } else if (value instanceof Integer) {
        return newDatum(DataType.INT32, int32Value((Integer) value));
      } else if (value instanceof Long) {
        return newDatum(DataType.INT64, int64Value((Long) value));
      } else if (value instanceof Float) {
        return newDatum(DataType.FLOAT, doubleValue((Float) value));
      } else if (value instanceof Double) {
        return newDatum(DataType.DOUBLE, doubleValue((Double) value));
      } else if (value instanceof String) {
        return newDatum(DataType.STRING, stringValue((String) value));
      } else if (value instanceof byte[]) {
        return newDatum(DataType.BINARY, binaryValue((byte[]) value));
      } else if (value instanceof ByteBuffer) {
        return newDatum(DataType.BINARY, binaryValue((ByteBuffer) value));
      } else {
        throw new RuntimeException("Unsupported datum type: " + value.getClass().getSimpleName()
            + ", value: " + value);
      }
    } else {
      assert value instanceof List;
      switch (repeatedType) {
      case BOOL:
        return newDatum(DataType.BOOL_SET, boolSetValue((List<Boolean>) value));
      case INT8:
        return newDatum(DataType.INT8_SET, int8SetValue((List<Byte>) value));
      case INT16:
        return newDatum(DataType.INT16_SET, int16SetValue((List<Short>) value));
      case INT32:
        return newDatum(DataType.INT32_SET, int32SetValue((List<Integer>) value));
      case INT64:
        return newDatum(DataType.INT64_SET, int64SetValue((List<Long>) value));
      case FLOAT:
        return newDatum(DataType.FLOAT_SET,
            doubleSetValue(fromFloatListToDoubleList((List<Float>) value)));
      case DOUBLE:
        return newDatum(DataType.DOUBLE_SET, doubleSetValue((List<Double>) value));
      case STRING:
        return newDatum(DataType.STRING_SET, stringSetValue((List<String>) value));
      case BINARY:
        if (((List) value).isEmpty()) {
          return newDatum(DataType.BINARY_SET, binarySetValue(new ArrayList<ByteBuffer>()));
        }
        if (((List) value).get(0) instanceof byte[]) {
          return newDatum(DataType.BINARY_SET,
              binarySetValue(fromByteArrayListToByteBufferList((List<byte[]>) value)));
        } else if (((List) value).get(0) instanceof ByteBuffer) {
          return newDatum(DataType.BINARY_SET, binarySetValue((List<ByteBuffer>) value));
        }
      default:
        throw new RuntimeException("Unsupported datum type: " + repeatedType);
      }
    }
  }

  private static Datum newDatum(DataType type, Value value) {
    return new Datum().setType(type).setValue(value);
  }

  private static List<Double> fromFloatListToDoubleList(List<Float> floatList) {
    List<Double> doubleList = new ArrayList<Double>();
    for (float e : floatList) {
      doubleList.add((double) e);
    }
    return doubleList;
  }

  private static List<ByteBuffer> fromByteArrayListToByteBufferList(List<byte[]> byteArrayList) {
    List<ByteBuffer> byteBufferList = new ArrayList<ByteBuffer>();
    for (byte[] e : byteArrayList) {
      byteBufferList.add(ByteBuffer.wrap(e));
    }
    return byteBufferList;
  }

  public static List<Map<String, Object>> fromDatum(List<Map<String, Datum>> keyValuesList) {
    if (keyValuesList != null) {
      List<Map<String, Object>> kvsList = new ArrayList<Map<String, Object>>();
      for (Map<String, Datum> kvs : keyValuesList) {
        kvsList.add(fromDatum(kvs));
      }
      return kvsList;
    }
    return null;
  }

  public static Map<String, Object> fromDatum(Map<String, Datum> keyValues) {
    if (keyValues != null) {
      Map<String, Object> kvs = new HashMap<String, Object>();
      for (Map.Entry<String, Datum> kv : keyValues.entrySet()) {
        kvs.put(kv.getKey(), fromDatum(kv.getValue()));
      }
      return kvs;
    }
    return null;
  }

  public static Number fromNumericDatum(Datum datum) throws IllegalArgumentException {
    if (datum != null) {
      Object value = fromDatum(datum);
      if (value == null || value instanceof Number) {
        return (Number) value;
      } else {
        throw new IllegalArgumentException(
            "Input datum is expected to be numeric value, but actual type is: "
                + value.getClass().getSimpleName()
        );
      }
    }
    return null;
  }

  public static Map<String, Number> fromNumericDatum(Map<String, Datum> keyValues)
      throws IllegalArgumentException {
    if (keyValues != null) {
      Map<String, Number> kvsBo = new HashMap<String, Number>();
      for (Map.Entry<String, Datum> kv : keyValues.entrySet()) {
        Object value = fromDatum(kv.getValue());
        if (value == null || value instanceof Number) {
          kvsBo.put(kv.getKey(), (Number) value);
        } else {
          throw new IllegalArgumentException(
              "Input datum is expected to be numeric value, but actual type is: "
                  + value.getClass().getSimpleName()
          );
        }
      }
      return kvsBo;
    }
    return null;
  }

  public static Object fromDatum(Datum datum) {
    if (datum == null) {
      // datum should not be null, keep silent and let service layer do further validation
      return null;
    }
    if (datum.getType() == null || datum.getValue() == null) {
      throw new IllegalArgumentException("Datum must has value and type");
    }
    Value value = datum.getValue();
    switch (datum.getType()) {
    case BOOL:
      return value.getBoolValue();
    case INT8:
      return value.getInt8Value();
    case INT16:
      return value.getInt16Value();
    case INT32:
      return value.getInt32Value();
    case INT64:
      return value.getInt64Value();
    case FLOAT:
      return (float) value.getDoubleValue();
    case DOUBLE:
      return value.getDoubleValue();
    case STRING:
      return value.getStringValue();
    case BINARY:
    case RAWBINARY:
      return value.getBinaryValue();
    case BOOL_SET:
      return fromListToSet(value.getBoolSetValue());
    case INT8_SET:
      return fromListToSet(value.getInt8SetValue());
    case INT16_SET:
      return fromListToSet(value.getInt16SetValue());
    case INT32_SET:
      return fromListToSet(value.getInt32SetValue());
    case INT64_SET:
      return fromListToSet(value.getInt64SetValue());
    case FLOAT_SET:
      return fromListToSetFloat(value.getDoubleSetValue());
    case DOUBLE_SET:
      return fromListToSet(value.getDoubleSetValue());
    case STRING_SET:
      return fromListToSet(value.getStringSetValue());
    case BINARY_SET:
      return fromListToSetByteArray(value.getBinarySetValue());
    default:
      throw new IllegalArgumentException("Unsupported datum type: " + datum.getType() + ", value: "
          + value);
    }
  }

  private static Set<Float> fromListToSetFloat(List<Double> list) {
    Set<Float> set = new HashSet<Float>();
    for (double e : list) {
      set.add((float) e);
    }
    return set;
  }

  private static Set<byte[]> fromListToSetByteArray(List<ByteBuffer> list) {
    Set<byte[]> set = new TreeSet<byte[]>(new Comparator<byte[]>() {
      @Override public int compare(byte[] left, byte[] right) {
        return compareTo(left, 0, left.length, right, 0, right.length);
      }

      public int compareTo(byte[] buffer1, int offset1, int length1,
          byte[] buffer2, int offset2, int length2) {
        if (buffer1 == buffer2 &&
            offset1 == offset2 &&
            length1 == length2) {
          return 0;
        }
        //WritableComparator code
        int end1 = offset1 + length1;
        int end2 = offset2 + length2;
        for (int i = offset1, j = offset2; i < end1 && j < end2; i++, j++) {
          int a = (buffer1[i] & 0xff);
          int b = (buffer2[j] & 0xff);
          if (a != b) {
            return a - b;
          }
        }
        return length1 - length2;
      }
    });
    for (ByteBuffer e : list) {
      set.add(e.array());
    }
    return set;
  }

  private static <T> Set<T> fromListToSet(List<T> list) {
    Set<T> set = new HashSet<T>();
    set.addAll(list);
    return set;
  }

  public static byte[] serialize(Datum datum) {
    try {
      TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
      return serializer.serialize(datum);
    } catch (TException te) {
      throw new RuntimeException("Failed to serialize thrift object: " + datum, te);
    }
  }

  public static Datum deserialize(byte[] bytes) {
    try {
      TDeserializer deserializer = new TDeserializer(new TCompactProtocol.Factory());
      Datum datum = new Datum();
      deserializer.deserialize(datum, bytes);
      return datum;
    } catch (TException te) {
      throw new RuntimeException("Failed to deserialize thrift object", te);
    }
  }

  public static byte[] serializeDatumMap(Map<String, Datum> record) {
    try {
      TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
      return serializer.serialize(new DatumMap().setData(record));
    } catch (TException te) {
      throw new RuntimeException("Failed to serialize thrift object: " + record, te);
    }
  }

  public static Map<String, Datum> deserializeDatumMap(byte[] bytes) {
    try {
      TDeserializer deserializer = new TDeserializer(new TCompactProtocol.Factory());
      DatumMap datumMap = new DatumMap();
      deserializer.deserialize(datumMap, bytes);
      return datumMap.getData();
    } catch (TException te) {
      throw new RuntimeException("Failed to deserialize thrift object", te);
    }
  }
}
