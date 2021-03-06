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
@Generated(value = "Autogenerated by Thrift Compiler (0.9.2)", date = "2019-3-7")
public class CreateClusterResponse implements libthrift091.TBase<CreateClusterResponse, CreateClusterResponse._Fields>, java.io.Serializable, Cloneable, Comparable<CreateClusterResponse> {
  private static final libthrift091.protocol.TStruct STRUCT_DESC = new libthrift091.protocol.TStruct("CreateClusterResponse");

  private static final libthrift091.protocol.TField CLUSTER_ID_FIELD_DESC = new libthrift091.protocol.TField("clusterId", libthrift091.protocol.TType.STRING, (short)1);
  private static final libthrift091.protocol.TField NAME_FIELD_DESC = new libthrift091.protocol.TField("name", libthrift091.protocol.TType.STRING, (short)2);
  private static final libthrift091.protocol.TField ADD_INSTANCE_GROUP_RESPONSES_FIELD_DESC = new libthrift091.protocol.TField("addInstanceGroupResponses", libthrift091.protocol.TType.LIST, (short)3);
  private static final libthrift091.protocol.TField SUBMIT_JOB_RESPONSES_FIELD_DESC = new libthrift091.protocol.TField("submitJobResponses", libthrift091.protocol.TType.LIST, (short)4);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new CreateClusterResponseStandardSchemeFactory());
    schemes.put(TupleScheme.class, new CreateClusterResponseTupleSchemeFactory());
  }

  public String clusterId; // required
  public String name; // required
  public List<AddInstanceGroupResponse> addInstanceGroupResponses; // optional
  public List<SubmitJobResponse> submitJobResponses; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements libthrift091.TFieldIdEnum {
    CLUSTER_ID((short)1, "clusterId"),
    NAME((short)2, "name"),
    ADD_INSTANCE_GROUP_RESPONSES((short)3, "addInstanceGroupResponses"),
    SUBMIT_JOB_RESPONSES((short)4, "submitJobResponses");

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
        case 1: // CLUSTER_ID
          return CLUSTER_ID;
        case 2: // NAME
          return NAME;
        case 3: // ADD_INSTANCE_GROUP_RESPONSES
          return ADD_INSTANCE_GROUP_RESPONSES;
        case 4: // SUBMIT_JOB_RESPONSES
          return SUBMIT_JOB_RESPONSES;
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
  private static final _Fields optionals[] = {_Fields.ADD_INSTANCE_GROUP_RESPONSES,_Fields.SUBMIT_JOB_RESPONSES};
  public static final Map<_Fields, libthrift091.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, libthrift091.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, libthrift091.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.CLUSTER_ID, new libthrift091.meta_data.FieldMetaData("clusterId", libthrift091.TFieldRequirementType.REQUIRED, 
        new libthrift091.meta_data.FieldValueMetaData(libthrift091.protocol.TType.STRING)));
    tmpMap.put(_Fields.NAME, new libthrift091.meta_data.FieldMetaData("name", libthrift091.TFieldRequirementType.REQUIRED, 
        new libthrift091.meta_data.FieldValueMetaData(libthrift091.protocol.TType.STRING)));
    tmpMap.put(_Fields.ADD_INSTANCE_GROUP_RESPONSES, new libthrift091.meta_data.FieldMetaData("addInstanceGroupResponses", libthrift091.TFieldRequirementType.OPTIONAL, 
        new libthrift091.meta_data.ListMetaData(libthrift091.protocol.TType.LIST, 
            new libthrift091.meta_data.StructMetaData(libthrift091.protocol.TType.STRUCT, AddInstanceGroupResponse.class))));
    tmpMap.put(_Fields.SUBMIT_JOB_RESPONSES, new libthrift091.meta_data.FieldMetaData("submitJobResponses", libthrift091.TFieldRequirementType.OPTIONAL, 
        new libthrift091.meta_data.ListMetaData(libthrift091.protocol.TType.LIST, 
            new libthrift091.meta_data.StructMetaData(libthrift091.protocol.TType.STRUCT, SubmitJobResponse.class))));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    libthrift091.meta_data.FieldMetaData.addStructMetaDataMap(CreateClusterResponse.class, metaDataMap);
  }

  public CreateClusterResponse() {
  }

  public CreateClusterResponse(
    String clusterId,
    String name)
  {
    this();
    this.clusterId = clusterId;
    this.name = name;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public CreateClusterResponse(CreateClusterResponse other) {
    if (other.isSetClusterId()) {
      this.clusterId = other.clusterId;
    }
    if (other.isSetName()) {
      this.name = other.name;
    }
    if (other.isSetAddInstanceGroupResponses()) {
      List<AddInstanceGroupResponse> __this__addInstanceGroupResponses = new ArrayList<AddInstanceGroupResponse>(other.addInstanceGroupResponses.size());
      for (AddInstanceGroupResponse other_element : other.addInstanceGroupResponses) {
        __this__addInstanceGroupResponses.add(new AddInstanceGroupResponse(other_element));
      }
      this.addInstanceGroupResponses = __this__addInstanceGroupResponses;
    }
    if (other.isSetSubmitJobResponses()) {
      List<SubmitJobResponse> __this__submitJobResponses = new ArrayList<SubmitJobResponse>(other.submitJobResponses.size());
      for (SubmitJobResponse other_element : other.submitJobResponses) {
        __this__submitJobResponses.add(new SubmitJobResponse(other_element));
      }
      this.submitJobResponses = __this__submitJobResponses;
    }
  }

  public CreateClusterResponse deepCopy() {
    return new CreateClusterResponse(this);
  }

  @Override
  public void clear() {
    this.clusterId = null;
    this.name = null;
    this.addInstanceGroupResponses = null;
    this.submitJobResponses = null;
  }

  public String getClusterId() {
    return this.clusterId;
  }

  public CreateClusterResponse setClusterId(String clusterId) {
    this.clusterId = clusterId;
    return this;
  }

  public void unsetClusterId() {
    this.clusterId = null;
  }

  /** Returns true if field clusterId is set (has been assigned a value) and false otherwise */
  public boolean isSetClusterId() {
    return this.clusterId != null;
  }

  public void setClusterIdIsSet(boolean value) {
    if (!value) {
      this.clusterId = null;
    }
  }

  public String getName() {
    return this.name;
  }

  public CreateClusterResponse setName(String name) {
    this.name = name;
    return this;
  }

  public void unsetName() {
    this.name = null;
  }

  /** Returns true if field name is set (has been assigned a value) and false otherwise */
  public boolean isSetName() {
    return this.name != null;
  }

  public void setNameIsSet(boolean value) {
    if (!value) {
      this.name = null;
    }
  }

  public int getAddInstanceGroupResponsesSize() {
    return (this.addInstanceGroupResponses == null) ? 0 : this.addInstanceGroupResponses.size();
  }

  public java.util.Iterator<AddInstanceGroupResponse> getAddInstanceGroupResponsesIterator() {
    return (this.addInstanceGroupResponses == null) ? null : this.addInstanceGroupResponses.iterator();
  }

  public void addToAddInstanceGroupResponses(AddInstanceGroupResponse elem) {
    if (this.addInstanceGroupResponses == null) {
      this.addInstanceGroupResponses = new ArrayList<AddInstanceGroupResponse>();
    }
    this.addInstanceGroupResponses.add(elem);
  }

  public List<AddInstanceGroupResponse> getAddInstanceGroupResponses() {
    return this.addInstanceGroupResponses;
  }

  public CreateClusterResponse setAddInstanceGroupResponses(List<AddInstanceGroupResponse> addInstanceGroupResponses) {
    this.addInstanceGroupResponses = addInstanceGroupResponses;
    return this;
  }

  public void unsetAddInstanceGroupResponses() {
    this.addInstanceGroupResponses = null;
  }

  /** Returns true if field addInstanceGroupResponses is set (has been assigned a value) and false otherwise */
  public boolean isSetAddInstanceGroupResponses() {
    return this.addInstanceGroupResponses != null;
  }

  public void setAddInstanceGroupResponsesIsSet(boolean value) {
    if (!value) {
      this.addInstanceGroupResponses = null;
    }
  }

  public int getSubmitJobResponsesSize() {
    return (this.submitJobResponses == null) ? 0 : this.submitJobResponses.size();
  }

  public java.util.Iterator<SubmitJobResponse> getSubmitJobResponsesIterator() {
    return (this.submitJobResponses == null) ? null : this.submitJobResponses.iterator();
  }

  public void addToSubmitJobResponses(SubmitJobResponse elem) {
    if (this.submitJobResponses == null) {
      this.submitJobResponses = new ArrayList<SubmitJobResponse>();
    }
    this.submitJobResponses.add(elem);
  }

  public List<SubmitJobResponse> getSubmitJobResponses() {
    return this.submitJobResponses;
  }

  public CreateClusterResponse setSubmitJobResponses(List<SubmitJobResponse> submitJobResponses) {
    this.submitJobResponses = submitJobResponses;
    return this;
  }

  public void unsetSubmitJobResponses() {
    this.submitJobResponses = null;
  }

  /** Returns true if field submitJobResponses is set (has been assigned a value) and false otherwise */
  public boolean isSetSubmitJobResponses() {
    return this.submitJobResponses != null;
  }

  public void setSubmitJobResponsesIsSet(boolean value) {
    if (!value) {
      this.submitJobResponses = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case CLUSTER_ID:
      if (value == null) {
        unsetClusterId();
      } else {
        setClusterId((String)value);
      }
      break;

    case NAME:
      if (value == null) {
        unsetName();
      } else {
        setName((String)value);
      }
      break;

    case ADD_INSTANCE_GROUP_RESPONSES:
      if (value == null) {
        unsetAddInstanceGroupResponses();
      } else {
        setAddInstanceGroupResponses((List<AddInstanceGroupResponse>)value);
      }
      break;

    case SUBMIT_JOB_RESPONSES:
      if (value == null) {
        unsetSubmitJobResponses();
      } else {
        setSubmitJobResponses((List<SubmitJobResponse>)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case CLUSTER_ID:
      return getClusterId();

    case NAME:
      return getName();

    case ADD_INSTANCE_GROUP_RESPONSES:
      return getAddInstanceGroupResponses();

    case SUBMIT_JOB_RESPONSES:
      return getSubmitJobResponses();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case CLUSTER_ID:
      return isSetClusterId();
    case NAME:
      return isSetName();
    case ADD_INSTANCE_GROUP_RESPONSES:
      return isSetAddInstanceGroupResponses();
    case SUBMIT_JOB_RESPONSES:
      return isSetSubmitJobResponses();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof CreateClusterResponse)
      return this.equals((CreateClusterResponse)that);
    return false;
  }

  public boolean equals(CreateClusterResponse that) {
    if (that == null)
      return false;

    boolean this_present_clusterId = true && this.isSetClusterId();
    boolean that_present_clusterId = true && that.isSetClusterId();
    if (this_present_clusterId || that_present_clusterId) {
      if (!(this_present_clusterId && that_present_clusterId))
        return false;
      if (!this.clusterId.equals(that.clusterId))
        return false;
    }

    boolean this_present_name = true && this.isSetName();
    boolean that_present_name = true && that.isSetName();
    if (this_present_name || that_present_name) {
      if (!(this_present_name && that_present_name))
        return false;
      if (!this.name.equals(that.name))
        return false;
    }

    boolean this_present_addInstanceGroupResponses = true && this.isSetAddInstanceGroupResponses();
    boolean that_present_addInstanceGroupResponses = true && that.isSetAddInstanceGroupResponses();
    if (this_present_addInstanceGroupResponses || that_present_addInstanceGroupResponses) {
      if (!(this_present_addInstanceGroupResponses && that_present_addInstanceGroupResponses))
        return false;
      if (!this.addInstanceGroupResponses.equals(that.addInstanceGroupResponses))
        return false;
    }

    boolean this_present_submitJobResponses = true && this.isSetSubmitJobResponses();
    boolean that_present_submitJobResponses = true && that.isSetSubmitJobResponses();
    if (this_present_submitJobResponses || that_present_submitJobResponses) {
      if (!(this_present_submitJobResponses && that_present_submitJobResponses))
        return false;
      if (!this.submitJobResponses.equals(that.submitJobResponses))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_clusterId = true && (isSetClusterId());
    list.add(present_clusterId);
    if (present_clusterId)
      list.add(clusterId);

    boolean present_name = true && (isSetName());
    list.add(present_name);
    if (present_name)
      list.add(name);

    boolean present_addInstanceGroupResponses = true && (isSetAddInstanceGroupResponses());
    list.add(present_addInstanceGroupResponses);
    if (present_addInstanceGroupResponses)
      list.add(addInstanceGroupResponses);

    boolean present_submitJobResponses = true && (isSetSubmitJobResponses());
    list.add(present_submitJobResponses);
    if (present_submitJobResponses)
      list.add(submitJobResponses);

    return list.hashCode();
  }

  @Override
  public int compareTo(CreateClusterResponse other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetClusterId()).compareTo(other.isSetClusterId());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetClusterId()) {
      lastComparison = libthrift091.TBaseHelper.compareTo(this.clusterId, other.clusterId);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetName()).compareTo(other.isSetName());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetName()) {
      lastComparison = libthrift091.TBaseHelper.compareTo(this.name, other.name);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetAddInstanceGroupResponses()).compareTo(other.isSetAddInstanceGroupResponses());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetAddInstanceGroupResponses()) {
      lastComparison = libthrift091.TBaseHelper.compareTo(this.addInstanceGroupResponses, other.addInstanceGroupResponses);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetSubmitJobResponses()).compareTo(other.isSetSubmitJobResponses());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetSubmitJobResponses()) {
      lastComparison = libthrift091.TBaseHelper.compareTo(this.submitJobResponses, other.submitJobResponses);
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
    StringBuilder sb = new StringBuilder("CreateClusterResponse(");
    boolean first = true;

    sb.append("clusterId:");
    if (this.clusterId == null) {
      sb.append("null");
    } else {
      sb.append(this.clusterId);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("name:");
    if (this.name == null) {
      sb.append("null");
    } else {
      sb.append(this.name);
    }
    first = false;
    if (isSetAddInstanceGroupResponses()) {
      if (!first) sb.append(", ");
      sb.append("addInstanceGroupResponses:");
      if (this.addInstanceGroupResponses == null) {
        sb.append("null");
      } else {
        sb.append(this.addInstanceGroupResponses);
      }
      first = false;
    }
    if (isSetSubmitJobResponses()) {
      if (!first) sb.append(", ");
      sb.append("submitJobResponses:");
      if (this.submitJobResponses == null) {
        sb.append("null");
      } else {
        sb.append(this.submitJobResponses);
      }
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws libthrift091.TException {
    // check for required fields
    if (clusterId == null) {
      throw new libthrift091.protocol.TProtocolException("Required field 'clusterId' was not present! Struct: " + toString());
    }
    if (name == null) {
      throw new libthrift091.protocol.TProtocolException("Required field 'name' was not present! Struct: " + toString());
    }
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

  private static class CreateClusterResponseStandardSchemeFactory implements SchemeFactory {
    public CreateClusterResponseStandardScheme getScheme() {
      return new CreateClusterResponseStandardScheme();
    }
  }

  private static class CreateClusterResponseStandardScheme extends StandardScheme<CreateClusterResponse> {

    public void read(libthrift091.protocol.TProtocol iprot, CreateClusterResponse struct) throws libthrift091.TException {
      libthrift091.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == libthrift091.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // CLUSTER_ID
            if (schemeField.type == libthrift091.protocol.TType.STRING) {
              struct.clusterId = iprot.readString();
              struct.setClusterIdIsSet(true);
            } else { 
              libthrift091.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // NAME
            if (schemeField.type == libthrift091.protocol.TType.STRING) {
              struct.name = iprot.readString();
              struct.setNameIsSet(true);
            } else { 
              libthrift091.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // ADD_INSTANCE_GROUP_RESPONSES
            if (schemeField.type == libthrift091.protocol.TType.LIST) {
              {
                libthrift091.protocol.TList _list68 = iprot.readListBegin();
                struct.addInstanceGroupResponses = new ArrayList<AddInstanceGroupResponse>(_list68.size);
                AddInstanceGroupResponse _elem69;
                for (int _i70 = 0; _i70 < _list68.size; ++_i70)
                {
                  _elem69 = new AddInstanceGroupResponse();
                  _elem69.read(iprot);
                  struct.addInstanceGroupResponses.add(_elem69);
                }
                iprot.readListEnd();
              }
              struct.setAddInstanceGroupResponsesIsSet(true);
            } else { 
              libthrift091.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // SUBMIT_JOB_RESPONSES
            if (schemeField.type == libthrift091.protocol.TType.LIST) {
              {
                libthrift091.protocol.TList _list71 = iprot.readListBegin();
                struct.submitJobResponses = new ArrayList<SubmitJobResponse>(_list71.size);
                SubmitJobResponse _elem72;
                for (int _i73 = 0; _i73 < _list71.size; ++_i73)
                {
                  _elem72 = new SubmitJobResponse();
                  _elem72.read(iprot);
                  struct.submitJobResponses.add(_elem72);
                }
                iprot.readListEnd();
              }
              struct.setSubmitJobResponsesIsSet(true);
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

    public void write(libthrift091.protocol.TProtocol oprot, CreateClusterResponse struct) throws libthrift091.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.clusterId != null) {
        oprot.writeFieldBegin(CLUSTER_ID_FIELD_DESC);
        oprot.writeString(struct.clusterId);
        oprot.writeFieldEnd();
      }
      if (struct.name != null) {
        oprot.writeFieldBegin(NAME_FIELD_DESC);
        oprot.writeString(struct.name);
        oprot.writeFieldEnd();
      }
      if (struct.addInstanceGroupResponses != null) {
        if (struct.isSetAddInstanceGroupResponses()) {
          oprot.writeFieldBegin(ADD_INSTANCE_GROUP_RESPONSES_FIELD_DESC);
          {
            oprot.writeListBegin(new libthrift091.protocol.TList(libthrift091.protocol.TType.STRUCT, struct.addInstanceGroupResponses.size()));
            for (AddInstanceGroupResponse _iter74 : struct.addInstanceGroupResponses)
            {
              _iter74.write(oprot);
            }
            oprot.writeListEnd();
          }
          oprot.writeFieldEnd();
        }
      }
      if (struct.submitJobResponses != null) {
        if (struct.isSetSubmitJobResponses()) {
          oprot.writeFieldBegin(SUBMIT_JOB_RESPONSES_FIELD_DESC);
          {
            oprot.writeListBegin(new libthrift091.protocol.TList(libthrift091.protocol.TType.STRUCT, struct.submitJobResponses.size()));
            for (SubmitJobResponse _iter75 : struct.submitJobResponses)
            {
              _iter75.write(oprot);
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

  private static class CreateClusterResponseTupleSchemeFactory implements SchemeFactory {
    public CreateClusterResponseTupleScheme getScheme() {
      return new CreateClusterResponseTupleScheme();
    }
  }

  private static class CreateClusterResponseTupleScheme extends TupleScheme<CreateClusterResponse> {

    @Override
    public void write(libthrift091.protocol.TProtocol prot, CreateClusterResponse struct) throws libthrift091.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      oprot.writeString(struct.clusterId);
      oprot.writeString(struct.name);
      BitSet optionals = new BitSet();
      if (struct.isSetAddInstanceGroupResponses()) {
        optionals.set(0);
      }
      if (struct.isSetSubmitJobResponses()) {
        optionals.set(1);
      }
      oprot.writeBitSet(optionals, 2);
      if (struct.isSetAddInstanceGroupResponses()) {
        {
          oprot.writeI32(struct.addInstanceGroupResponses.size());
          for (AddInstanceGroupResponse _iter76 : struct.addInstanceGroupResponses)
          {
            _iter76.write(oprot);
          }
        }
      }
      if (struct.isSetSubmitJobResponses()) {
        {
          oprot.writeI32(struct.submitJobResponses.size());
          for (SubmitJobResponse _iter77 : struct.submitJobResponses)
          {
            _iter77.write(oprot);
          }
        }
      }
    }

    @Override
    public void read(libthrift091.protocol.TProtocol prot, CreateClusterResponse struct) throws libthrift091.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      struct.clusterId = iprot.readString();
      struct.setClusterIdIsSet(true);
      struct.name = iprot.readString();
      struct.setNameIsSet(true);
      BitSet incoming = iprot.readBitSet(2);
      if (incoming.get(0)) {
        {
          libthrift091.protocol.TList _list78 = new libthrift091.protocol.TList(libthrift091.protocol.TType.STRUCT, iprot.readI32());
          struct.addInstanceGroupResponses = new ArrayList<AddInstanceGroupResponse>(_list78.size);
          AddInstanceGroupResponse _elem79;
          for (int _i80 = 0; _i80 < _list78.size; ++_i80)
          {
            _elem79 = new AddInstanceGroupResponse();
            _elem79.read(iprot);
            struct.addInstanceGroupResponses.add(_elem79);
          }
        }
        struct.setAddInstanceGroupResponsesIsSet(true);
      }
      if (incoming.get(1)) {
        {
          libthrift091.protocol.TList _list81 = new libthrift091.protocol.TList(libthrift091.protocol.TType.STRUCT, iprot.readI32());
          struct.submitJobResponses = new ArrayList<SubmitJobResponse>(_list81.size);
          SubmitJobResponse _elem82;
          for (int _i83 = 0; _i83 < _list81.size; ++_i83)
          {
            _elem82 = new SubmitJobResponse();
            _elem82.read(iprot);
            struct.submitJobResponses.add(_elem82);
          }
        }
        struct.setSubmitJobResponsesIsSet(true);
      }
    }
  }

}

