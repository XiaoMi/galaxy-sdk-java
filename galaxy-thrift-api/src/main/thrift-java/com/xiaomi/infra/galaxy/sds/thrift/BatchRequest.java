/**
 * Autogenerated by Thrift Compiler (0.9.2)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package com.xiaomi.infra.galaxy.sds.thrift;

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
@Generated(value = "Autogenerated by Thrift Compiler (0.9.2)", date = "2015-1-13")
public class BatchRequest implements libthrift091.TBase<BatchRequest, BatchRequest._Fields>, java.io.Serializable, Cloneable, Comparable<BatchRequest> {
  private static final libthrift091.protocol.TStruct STRUCT_DESC = new libthrift091.protocol.TStruct("BatchRequest");

  private static final libthrift091.protocol.TField ITEMS_FIELD_DESC = new libthrift091.protocol.TField("items", libthrift091.protocol.TType.LIST, (short)1);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new BatchRequestStandardSchemeFactory());
    schemes.put(TupleScheme.class, new BatchRequestTupleSchemeFactory());
  }

  public List<BatchRequestItem> items; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements libthrift091.TFieldIdEnum {
    ITEMS((short)1, "items");

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
        case 1: // ITEMS
          return ITEMS;
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
  private static final _Fields optionals[] = {_Fields.ITEMS};
  public static final Map<_Fields, libthrift091.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, libthrift091.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, libthrift091.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.ITEMS, new libthrift091.meta_data.FieldMetaData("items", libthrift091.TFieldRequirementType.OPTIONAL, 
        new libthrift091.meta_data.ListMetaData(libthrift091.protocol.TType.LIST, 
            new libthrift091.meta_data.StructMetaData(libthrift091.protocol.TType.STRUCT, BatchRequestItem.class))));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    libthrift091.meta_data.FieldMetaData.addStructMetaDataMap(BatchRequest.class, metaDataMap);
  }

  public BatchRequest() {
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public BatchRequest(BatchRequest other) {
    if (other.isSetItems()) {
      List<BatchRequestItem> __this__items = new ArrayList<BatchRequestItem>(other.items.size());
      for (BatchRequestItem other_element : other.items) {
        __this__items.add(new BatchRequestItem(other_element));
      }
      this.items = __this__items;
    }
  }

  public BatchRequest deepCopy() {
    return new BatchRequest(this);
  }

  @Override
  public void clear() {
    this.items = null;
  }

  public int getItemsSize() {
    return (this.items == null) ? 0 : this.items.size();
  }

  public java.util.Iterator<BatchRequestItem> getItemsIterator() {
    return (this.items == null) ? null : this.items.iterator();
  }

  public void addToItems(BatchRequestItem elem) {
    if (this.items == null) {
      this.items = new ArrayList<BatchRequestItem>();
    }
    this.items.add(elem);
  }

  public List<BatchRequestItem> getItems() {
    return this.items;
  }

  public BatchRequest setItems(List<BatchRequestItem> items) {
    this.items = items;
    return this;
  }

  public void unsetItems() {
    this.items = null;
  }

  /** Returns true if field items is set (has been assigned a value) and false otherwise */
  public boolean isSetItems() {
    return this.items != null;
  }

  public void setItemsIsSet(boolean value) {
    if (!value) {
      this.items = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case ITEMS:
      if (value == null) {
        unsetItems();
      } else {
        setItems((List<BatchRequestItem>)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case ITEMS:
      return getItems();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case ITEMS:
      return isSetItems();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof BatchRequest)
      return this.equals((BatchRequest)that);
    return false;
  }

  public boolean equals(BatchRequest that) {
    if (that == null)
      return false;

    boolean this_present_items = true && this.isSetItems();
    boolean that_present_items = true && that.isSetItems();
    if (this_present_items || that_present_items) {
      if (!(this_present_items && that_present_items))
        return false;
      if (!this.items.equals(that.items))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_items = true && (isSetItems());
    list.add(present_items);
    if (present_items)
      list.add(items);

    return list.hashCode();
  }

  @Override
  public int compareTo(BatchRequest other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetItems()).compareTo(other.isSetItems());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetItems()) {
      lastComparison = libthrift091.TBaseHelper.compareTo(this.items, other.items);
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
    StringBuilder sb = new StringBuilder("BatchRequest(");
    boolean first = true;

    if (isSetItems()) {
      sb.append("items:");
      if (this.items == null) {
        sb.append("null");
      } else {
        sb.append(this.items);
      }
      first = false;
    }
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

  private static class BatchRequestStandardSchemeFactory implements SchemeFactory {
    public BatchRequestStandardScheme getScheme() {
      return new BatchRequestStandardScheme();
    }
  }

  private static class BatchRequestStandardScheme extends StandardScheme<BatchRequest> {

    public void read(libthrift091.protocol.TProtocol iprot, BatchRequest struct) throws libthrift091.TException {
      libthrift091.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == libthrift091.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // ITEMS
            if (schemeField.type == libthrift091.protocol.TType.LIST) {
              {
                libthrift091.protocol.TList _list296 = iprot.readListBegin();
                struct.items = new ArrayList<BatchRequestItem>(_list296.size);
                BatchRequestItem _elem297;
                for (int _i298 = 0; _i298 < _list296.size; ++_i298)
                {
                  _elem297 = new BatchRequestItem();
                  _elem297.read(iprot);
                  struct.items.add(_elem297);
                }
                iprot.readListEnd();
              }
              struct.setItemsIsSet(true);
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

    public void write(libthrift091.protocol.TProtocol oprot, BatchRequest struct) throws libthrift091.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.items != null) {
        if (struct.isSetItems()) {
          oprot.writeFieldBegin(ITEMS_FIELD_DESC);
          {
            oprot.writeListBegin(new libthrift091.protocol.TList(libthrift091.protocol.TType.STRUCT, struct.items.size()));
            for (BatchRequestItem _iter299 : struct.items)
            {
              _iter299.write(oprot);
            }
            oprot.writeListEnd();
          }
          oprot.writeFieldEnd();
        }
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class BatchRequestTupleSchemeFactory implements SchemeFactory {
    public BatchRequestTupleScheme getScheme() {
      return new BatchRequestTupleScheme();
    }
  }

  private static class BatchRequestTupleScheme extends TupleScheme<BatchRequest> {

    @Override
    public void write(libthrift091.protocol.TProtocol prot, BatchRequest struct) throws libthrift091.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetItems()) {
        optionals.set(0);
      }
      oprot.writeBitSet(optionals, 1);
      if (struct.isSetItems()) {
        {
          oprot.writeI32(struct.items.size());
          for (BatchRequestItem _iter300 : struct.items)
          {
            _iter300.write(oprot);
          }
        }
      }
    }

    @Override
    public void read(libthrift091.protocol.TProtocol prot, BatchRequest struct) throws libthrift091.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(1);
      if (incoming.get(0)) {
        {
          libthrift091.protocol.TList _list301 = new libthrift091.protocol.TList(libthrift091.protocol.TType.STRUCT, iprot.readI32());
          struct.items = new ArrayList<BatchRequestItem>(_list301.size);
          BatchRequestItem _elem302;
          for (int _i303 = 0; _i303 < _list301.size; ++_i303)
          {
            _elem302 = new BatchRequestItem();
            _elem302.read(iprot);
            struct.items.add(_elem302);
          }
        }
        struct.setItemsIsSet(true);
      }
    }
  }

}

