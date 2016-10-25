/**
 * Autogenerated by Thrift Compiler (0.9.2)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package com.xiaomi.infra.galaxy.talos.thrift;

import libthrift091.scheme.IScheme;
import libthrift091.scheme.SchemeFactory;
import libthrift091.scheme.StandardScheme;

import libthrift091.scheme.TupleScheme;
import libthrift091.protocol.TTupleProtocol;
import libthrift091.protocol.TProtocolException;
import libthrift091.EncodingUtils;
import libthrift091.TException;
import libthrift091.async.AsyncMethodCallback;
import libthrift091.server.AbstractNonblockingServer.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;
import java.nio.ByteBuffer;
import java.util.Arrays;
import javax.annotation.Generated;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked"})
@Generated(value = "Autogenerated by Thrift Compiler (0.9.2)", date = "2016-10-25")
public class UnlockPartitionRequest implements libthrift091.TBase<UnlockPartitionRequest, UnlockPartitionRequest._Fields>, java.io.Serializable, Cloneable, Comparable<UnlockPartitionRequest> {
  private static final libthrift091.protocol.TStruct STRUCT_DESC = new libthrift091.protocol.TStruct("UnlockPartitionRequest");

  private static final libthrift091.protocol.TField CONSUME_UNIT_FIELD_DESC = new libthrift091.protocol.TField("consumeUnit", libthrift091.protocol.TType.STRUCT, (short)1);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new UnlockPartitionRequestStandardSchemeFactory());
    schemes.put(TupleScheme.class, new UnlockPartitionRequestTupleSchemeFactory());
  }

  /**
   * consumeUnit instance
   * 
   */
  public ConsumeUnit consumeUnit; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements libthrift091.TFieldIdEnum {
    /**
     * consumeUnit instance
     * 
     */
    CONSUME_UNIT((short)1, "consumeUnit");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // CONSUME_UNIT
          return CONSUME_UNIT;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    public static _Fields findByName(String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final String _fieldName;

    _Fields(short thriftId, String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  public static final Map<_Fields, libthrift091.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, libthrift091.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, libthrift091.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.CONSUME_UNIT, new libthrift091.meta_data.FieldMetaData("consumeUnit", libthrift091.TFieldRequirementType.REQUIRED, 
        new libthrift091.meta_data.StructMetaData(libthrift091.protocol.TType.STRUCT, ConsumeUnit.class)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    libthrift091.meta_data.FieldMetaData.addStructMetaDataMap(UnlockPartitionRequest.class, metaDataMap);
  }

  public UnlockPartitionRequest() {
  }

  public UnlockPartitionRequest(
    ConsumeUnit consumeUnit)
  {
    this();
    this.consumeUnit = consumeUnit;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public UnlockPartitionRequest(UnlockPartitionRequest other) {
    if (other.isSetConsumeUnit()) {
      this.consumeUnit = new ConsumeUnit(other.consumeUnit);
    }
  }

  public UnlockPartitionRequest deepCopy() {
    return new UnlockPartitionRequest(this);
  }

  @Override
  public void clear() {
    this.consumeUnit = null;
  }

  /**
   * consumeUnit instance
   * 
   */
  public ConsumeUnit getConsumeUnit() {
    return this.consumeUnit;
  }

  /**
   * consumeUnit instance
   * 
   */
  public UnlockPartitionRequest setConsumeUnit(ConsumeUnit consumeUnit) {
    this.consumeUnit = consumeUnit;
    return this;
  }

  public void unsetConsumeUnit() {
    this.consumeUnit = null;
  }

  /** Returns true if field consumeUnit is set (has been assigned a value) and false otherwise */
  public boolean isSetConsumeUnit() {
    return this.consumeUnit != null;
  }

  public void setConsumeUnitIsSet(boolean value) {
    if (!value) {
      this.consumeUnit = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case CONSUME_UNIT:
      if (value == null) {
        unsetConsumeUnit();
      } else {
        setConsumeUnit((ConsumeUnit)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case CONSUME_UNIT:
      return getConsumeUnit();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case CONSUME_UNIT:
      return isSetConsumeUnit();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof UnlockPartitionRequest)
      return this.equals((UnlockPartitionRequest)that);
    return false;
  }

  public boolean equals(UnlockPartitionRequest that) {
    if (that == null)
      return false;

    boolean this_present_consumeUnit = true && this.isSetConsumeUnit();
    boolean that_present_consumeUnit = true && that.isSetConsumeUnit();
    if (this_present_consumeUnit || that_present_consumeUnit) {
      if (!(this_present_consumeUnit && that_present_consumeUnit))
        return false;
      if (!this.consumeUnit.equals(that.consumeUnit))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_consumeUnit = true && (isSetConsumeUnit());
    list.add(present_consumeUnit);
    if (present_consumeUnit)
      list.add(consumeUnit);

    return list.hashCode();
  }

  @Override
  public int compareTo(UnlockPartitionRequest other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetConsumeUnit()).compareTo(other.isSetConsumeUnit());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetConsumeUnit()) {
      lastComparison = libthrift091.TBaseHelper.compareTo(this.consumeUnit, other.consumeUnit);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(libthrift091.protocol.TProtocol iprot) throws libthrift091.TException {
    schemes.get(iprot.getScheme()).getScheme().read(iprot, this);
  }

  public void write(libthrift091.protocol.TProtocol oprot) throws libthrift091.TException {
    schemes.get(oprot.getScheme()).getScheme().write(oprot, this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("UnlockPartitionRequest(");
    boolean first = true;

    sb.append("consumeUnit:");
    if (this.consumeUnit == null) {
      sb.append("null");
    } else {
      sb.append(this.consumeUnit);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws libthrift091.TException {
    // check for required fields
    if (consumeUnit == null) {
      throw new libthrift091.protocol.TProtocolException("Required field 'consumeUnit' was not present! Struct: " + toString());
    }
    // check for sub-struct validity
    if (consumeUnit != null) {
      consumeUnit.validate();
    }
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new libthrift091.protocol.TCompactProtocol(new libthrift091.transport.TIOStreamTransport(out)));
    } catch (libthrift091.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
    try {
      read(new libthrift091.protocol.TCompactProtocol(new libthrift091.transport.TIOStreamTransport(in)));
    } catch (libthrift091.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class UnlockPartitionRequestStandardSchemeFactory implements SchemeFactory {
    public UnlockPartitionRequestStandardScheme getScheme() {
      return new UnlockPartitionRequestStandardScheme();
    }
  }

  private static class UnlockPartitionRequestStandardScheme extends StandardScheme<UnlockPartitionRequest> {

    public void read(libthrift091.protocol.TProtocol iprot, UnlockPartitionRequest struct) throws libthrift091.TException {
      libthrift091.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == libthrift091.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // CONSUME_UNIT
            if (schemeField.type == libthrift091.protocol.TType.STRUCT) {
              struct.consumeUnit = new ConsumeUnit();
              struct.consumeUnit.read(iprot);
              struct.setConsumeUnitIsSet(true);
            } else { 
              libthrift091.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            libthrift091.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();

      // check for required fields of primitive type, which can't be checked in the validate method
      struct.validate();
    }

    public void write(libthrift091.protocol.TProtocol oprot, UnlockPartitionRequest struct) throws libthrift091.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.consumeUnit != null) {
        oprot.writeFieldBegin(CONSUME_UNIT_FIELD_DESC);
        struct.consumeUnit.write(oprot);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class UnlockPartitionRequestTupleSchemeFactory implements SchemeFactory {
    public UnlockPartitionRequestTupleScheme getScheme() {
      return new UnlockPartitionRequestTupleScheme();
    }
  }

  private static class UnlockPartitionRequestTupleScheme extends TupleScheme<UnlockPartitionRequest> {

    @Override
    public void write(libthrift091.protocol.TProtocol prot, UnlockPartitionRequest struct) throws libthrift091.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      struct.consumeUnit.write(oprot);
    }

    @Override
    public void read(libthrift091.protocol.TProtocol prot, UnlockPartitionRequest struct) throws libthrift091.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      struct.consumeUnit = new ConsumeUnit();
      struct.consumeUnit.read(iprot);
      struct.setConsumeUnitIsSet(true);
    }
  }

}

