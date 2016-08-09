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
public class DetailBill implements libthrift091.TBase<DetailBill, DetailBill._Fields>, java.io.Serializable, Cloneable, Comparable<DetailBill> {
  private static final libthrift091.protocol.TStruct STRUCT_DESC = new libthrift091.protocol.TStruct("DetailBill");

  private static final libthrift091.protocol.TField BILL_LIST_FIELD_DESC = new libthrift091.protocol.TField("billList", libthrift091.protocol.TType.LIST, (short)1);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new DetailBillStandardSchemeFactory());
    schemes.put(TupleScheme.class, new DetailBillTupleSchemeFactory());
  }

  public List<DetailBillItem> billList; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements libthrift091.TFieldIdEnum {
    BILL_LIST((short)1, "billList");

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
        case 1: // BILL_LIST
          return BILL_LIST;
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
    tmpMap.put(_Fields.BILL_LIST, new libthrift091.meta_data.FieldMetaData("billList", libthrift091.TFieldRequirementType.DEFAULT, 
        new libthrift091.meta_data.ListMetaData(libthrift091.protocol.TType.LIST, 
            new libthrift091.meta_data.StructMetaData(libthrift091.protocol.TType.STRUCT, DetailBillItem.class))));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    libthrift091.meta_data.FieldMetaData.addStructMetaDataMap(DetailBill.class, metaDataMap);
  }

  public DetailBill() {
  }

  public DetailBill(
    List<DetailBillItem> billList)
  {
    this();
    this.billList = billList;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public DetailBill(DetailBill other) {
    if (other.isSetBillList()) {
      List<DetailBillItem> __this__billList = new ArrayList<DetailBillItem>(other.billList.size());
      for (DetailBillItem other_element : other.billList) {
        __this__billList.add(new DetailBillItem(other_element));
      }
      this.billList = __this__billList;
    }
  }

  public DetailBill deepCopy() {
    return new DetailBill(this);
  }

  @Override
  public void clear() {
    this.billList = null;
  }

  public int getBillListSize() {
    return (this.billList == null) ? 0 : this.billList.size();
  }

  public java.util.Iterator<DetailBillItem> getBillListIterator() {
    return (this.billList == null) ? null : this.billList.iterator();
  }

  public void addToBillList(DetailBillItem elem) {
    if (this.billList == null) {
      this.billList = new ArrayList<DetailBillItem>();
    }
    this.billList.add(elem);
  }

  public List<DetailBillItem> getBillList() {
    return this.billList;
  }

  public DetailBill setBillList(List<DetailBillItem> billList) {
    this.billList = billList;
    return this;
  }

  public void unsetBillList() {
    this.billList = null;
  }

  /** Returns true if field billList is set (has been assigned a value) and false otherwise */
  public boolean isSetBillList() {
    return this.billList != null;
  }

  public void setBillListIsSet(boolean value) {
    if (!value) {
      this.billList = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case BILL_LIST:
      if (value == null) {
        unsetBillList();
      } else {
        setBillList((List<DetailBillItem>)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case BILL_LIST:
      return getBillList();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case BILL_LIST:
      return isSetBillList();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof DetailBill)
      return this.equals((DetailBill)that);
    return false;
  }

  public boolean equals(DetailBill that) {
    if (that == null)
      return false;

    boolean this_present_billList = true && this.isSetBillList();
    boolean that_present_billList = true && that.isSetBillList();
    if (this_present_billList || that_present_billList) {
      if (!(this_present_billList && that_present_billList))
        return false;
      if (!this.billList.equals(that.billList))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_billList = true && (isSetBillList());
    list.add(present_billList);
    if (present_billList)
      list.add(billList);

    return list.hashCode();
  }

  @Override
  public int compareTo(DetailBill other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetBillList()).compareTo(other.isSetBillList());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetBillList()) {
      lastComparison = libthrift091.TBaseHelper.compareTo(this.billList, other.billList);
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
    StringBuilder sb = new StringBuilder("DetailBill(");
    boolean first = true;

    sb.append("billList:");
    if (this.billList == null) {
      sb.append("null");
    } else {
      sb.append(this.billList);
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

  private static class DetailBillStandardSchemeFactory implements SchemeFactory {
    public DetailBillStandardScheme getScheme() {
      return new DetailBillStandardScheme();
    }
  }

  private static class DetailBillStandardScheme extends StandardScheme<DetailBill> {

    public void read(libthrift091.protocol.TProtocol iprot, DetailBill struct) throws libthrift091.TException {
      libthrift091.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == libthrift091.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // BILL_LIST
            if (schemeField.type == libthrift091.protocol.TType.LIST) {
              {
                libthrift091.protocol.TList _list16 = iprot.readListBegin();
                struct.billList = new ArrayList<DetailBillItem>(_list16.size);
                DetailBillItem _elem17;
                for (int _i18 = 0; _i18 < _list16.size; ++_i18)
                {
                  _elem17 = new DetailBillItem();
                  _elem17.read(iprot);
                  struct.billList.add(_elem17);
                }
                iprot.readListEnd();
              }
              struct.setBillListIsSet(true);
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

    public void write(libthrift091.protocol.TProtocol oprot, DetailBill struct) throws libthrift091.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.billList != null) {
        oprot.writeFieldBegin(BILL_LIST_FIELD_DESC);
        {
          oprot.writeListBegin(new libthrift091.protocol.TList(libthrift091.protocol.TType.STRUCT, struct.billList.size()));
          for (DetailBillItem _iter19 : struct.billList)
          {
            _iter19.write(oprot);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class DetailBillTupleSchemeFactory implements SchemeFactory {
    public DetailBillTupleScheme getScheme() {
      return new DetailBillTupleScheme();
    }
  }

  private static class DetailBillTupleScheme extends TupleScheme<DetailBill> {

    @Override
    public void write(libthrift091.protocol.TProtocol prot, DetailBill struct) throws libthrift091.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetBillList()) {
        optionals.set(0);
      }
      oprot.writeBitSet(optionals, 1);
      if (struct.isSetBillList()) {
        {
          oprot.writeI32(struct.billList.size());
          for (DetailBillItem _iter20 : struct.billList)
          {
            _iter20.write(oprot);
          }
        }
      }
    }

    @Override
    public void read(libthrift091.protocol.TProtocol prot, DetailBill struct) throws libthrift091.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(1);
      if (incoming.get(0)) {
        {
          libthrift091.protocol.TList _list21 = new libthrift091.protocol.TList(libthrift091.protocol.TType.STRUCT, iprot.readI32());
          struct.billList = new ArrayList<DetailBillItem>(_list21.size);
          DetailBillItem _elem22;
          for (int _i23 = 0; _i23 < _list21.size; ++_i23)
          {
            _elem22 = new DetailBillItem();
            _elem22.read(iprot);
            struct.billList.add(_elem22);
          }
        }
        struct.setBillListIsSet(true);
      }
    }
  }

}

