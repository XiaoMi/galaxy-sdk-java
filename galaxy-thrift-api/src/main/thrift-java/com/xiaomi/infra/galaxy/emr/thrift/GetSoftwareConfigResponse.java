/**
 * Autogenerated by Thrift Compiler (0.9.2)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package com.xiaomi.infra.galaxy.emr.thrift;

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
@Generated(value = "Autogenerated by Thrift Compiler (0.9.2)", date = "2016-8-2")
public class GetSoftwareConfigResponse implements libthrift091.TBase<GetSoftwareConfigResponse, GetSoftwareConfigResponse._Fields>, java.io.Serializable, Cloneable, Comparable<GetSoftwareConfigResponse> {
  private static final libthrift091.protocol.TStruct STRUCT_DESC = new libthrift091.protocol.TStruct("GetSoftwareConfigResponse");

  private static final libthrift091.protocol.TField SOFTWARE_FIELD_DESC = new libthrift091.protocol.TField("software", libthrift091.protocol.TType.LIST, (short)1);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new GetSoftwareConfigResponseStandardSchemeFactory());
    schemes.put(TupleScheme.class, new GetSoftwareConfigResponseTupleSchemeFactory());
  }

  public List<ApplicationSuite> software; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements libthrift091.TFieldIdEnum {
    SOFTWARE((short)1, "software");

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
        case 1: // SOFTWARE
          return SOFTWARE;
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
    tmpMap.put(_Fields.SOFTWARE, new libthrift091.meta_data.FieldMetaData("software", libthrift091.TFieldRequirementType.DEFAULT, 
        new libthrift091.meta_data.ListMetaData(libthrift091.protocol.TType.LIST, 
            new libthrift091.meta_data.StructMetaData(libthrift091.protocol.TType.STRUCT, ApplicationSuite.class))));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    libthrift091.meta_data.FieldMetaData.addStructMetaDataMap(GetSoftwareConfigResponse.class, metaDataMap);
  }

  public GetSoftwareConfigResponse() {
  }

  public GetSoftwareConfigResponse(
    List<ApplicationSuite> software)
  {
    this();
    this.software = software;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public GetSoftwareConfigResponse(GetSoftwareConfigResponse other) {
    if (other.isSetSoftware()) {
      List<ApplicationSuite> __this__software = new ArrayList<ApplicationSuite>(other.software.size());
      for (ApplicationSuite other_element : other.software) {
        __this__software.add(new ApplicationSuite(other_element));
      }
      this.software = __this__software;
    }
  }

  public GetSoftwareConfigResponse deepCopy() {
    return new GetSoftwareConfigResponse(this);
  }

  @Override
  public void clear() {
    this.software = null;
  }

  public int getSoftwareSize() {
    return (this.software == null) ? 0 : this.software.size();
  }

  public java.util.Iterator<ApplicationSuite> getSoftwareIterator() {
    return (this.software == null) ? null : this.software.iterator();
  }

  public void addToSoftware(ApplicationSuite elem) {
    if (this.software == null) {
      this.software = new ArrayList<ApplicationSuite>();
    }
    this.software.add(elem);
  }

  public List<ApplicationSuite> getSoftware() {
    return this.software;
  }

  public GetSoftwareConfigResponse setSoftware(List<ApplicationSuite> software) {
    this.software = software;
    return this;
  }

  public void unsetSoftware() {
    this.software = null;
  }

  /** Returns true if field software is set (has been assigned a value) and false otherwise */
  public boolean isSetSoftware() {
    return this.software != null;
  }

  public void setSoftwareIsSet(boolean value) {
    if (!value) {
      this.software = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case SOFTWARE:
      if (value == null) {
        unsetSoftware();
      } else {
        setSoftware((List<ApplicationSuite>)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case SOFTWARE:
      return getSoftware();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case SOFTWARE:
      return isSetSoftware();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof GetSoftwareConfigResponse)
      return this.equals((GetSoftwareConfigResponse)that);
    return false;
  }

  public boolean equals(GetSoftwareConfigResponse that) {
    if (that == null)
      return false;

    boolean this_present_software = true && this.isSetSoftware();
    boolean that_present_software = true && that.isSetSoftware();
    if (this_present_software || that_present_software) {
      if (!(this_present_software && that_present_software))
        return false;
      if (!this.software.equals(that.software))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_software = true && (isSetSoftware());
    list.add(present_software);
    if (present_software)
      list.add(software);

    return list.hashCode();
  }

  @Override
  public int compareTo(GetSoftwareConfigResponse other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetSoftware()).compareTo(other.isSetSoftware());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetSoftware()) {
      lastComparison = libthrift091.TBaseHelper.compareTo(this.software, other.software);
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
    StringBuilder sb = new StringBuilder("GetSoftwareConfigResponse(");
    boolean first = true;

    sb.append("software:");
    if (this.software == null) {
      sb.append("null");
    } else {
      sb.append(this.software);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws libthrift091.TException {
    // check for required fields
    // check for sub-struct validity
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

  private static class GetSoftwareConfigResponseStandardSchemeFactory implements SchemeFactory {
    public GetSoftwareConfigResponseStandardScheme getScheme() {
      return new GetSoftwareConfigResponseStandardScheme();
    }
  }

  private static class GetSoftwareConfigResponseStandardScheme extends StandardScheme<GetSoftwareConfigResponse> {

    public void read(libthrift091.protocol.TProtocol iprot, GetSoftwareConfigResponse struct) throws libthrift091.TException {
      libthrift091.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == libthrift091.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // SOFTWARE
            if (schemeField.type == libthrift091.protocol.TType.LIST) {
              {
                libthrift091.protocol.TList _list134 = iprot.readListBegin();
                struct.software = new ArrayList<ApplicationSuite>(_list134.size);
                ApplicationSuite _elem135;
                for (int _i136 = 0; _i136 < _list134.size; ++_i136)
                {
                  _elem135 = new ApplicationSuite();
                  _elem135.read(iprot);
                  struct.software.add(_elem135);
                }
                iprot.readListEnd();
              }
              struct.setSoftwareIsSet(true);
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

    public void write(libthrift091.protocol.TProtocol oprot, GetSoftwareConfigResponse struct) throws libthrift091.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.software != null) {
        oprot.writeFieldBegin(SOFTWARE_FIELD_DESC);
        {
          oprot.writeListBegin(new libthrift091.protocol.TList(libthrift091.protocol.TType.STRUCT, struct.software.size()));
          for (ApplicationSuite _iter137 : struct.software)
          {
            _iter137.write(oprot);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class GetSoftwareConfigResponseTupleSchemeFactory implements SchemeFactory {
    public GetSoftwareConfigResponseTupleScheme getScheme() {
      return new GetSoftwareConfigResponseTupleScheme();
    }
  }

  private static class GetSoftwareConfigResponseTupleScheme extends TupleScheme<GetSoftwareConfigResponse> {

    @Override
    public void write(libthrift091.protocol.TProtocol prot, GetSoftwareConfigResponse struct) throws libthrift091.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetSoftware()) {
        optionals.set(0);
      }
      oprot.writeBitSet(optionals, 1);
      if (struct.isSetSoftware()) {
        {
          oprot.writeI32(struct.software.size());
          for (ApplicationSuite _iter138 : struct.software)
          {
            _iter138.write(oprot);
          }
        }
      }
    }

    @Override
    public void read(libthrift091.protocol.TProtocol prot, GetSoftwareConfigResponse struct) throws libthrift091.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(1);
      if (incoming.get(0)) {
        {
          libthrift091.protocol.TList _list139 = new libthrift091.protocol.TList(libthrift091.protocol.TType.STRUCT, iprot.readI32());
          struct.software = new ArrayList<ApplicationSuite>(_list139.size);
          ApplicationSuite _elem140;
          for (int _i141 = 0; _i141 < _list139.size; ++_i141)
          {
            _elem140 = new ApplicationSuite();
            _elem140.read(iprot);
            struct.software.add(_elem140);
          }
        }
        struct.setSoftwareIsSet(true);
      }
    }
  }

}

