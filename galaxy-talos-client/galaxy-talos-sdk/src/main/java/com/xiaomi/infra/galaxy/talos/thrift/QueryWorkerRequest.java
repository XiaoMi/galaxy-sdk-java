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
public class QueryWorkerRequest implements libthrift091.TBase<QueryWorkerRequest, QueryWorkerRequest._Fields>, java.io.Serializable, Cloneable, Comparable<QueryWorkerRequest> {
  private static final libthrift091.protocol.TStruct STRUCT_DESC = new libthrift091.protocol.TStruct("QueryWorkerRequest");

  private static final libthrift091.protocol.TField CONSUMER_GROUP_FIELD_DESC = new libthrift091.protocol.TField("consumerGroup", libthrift091.protocol.TType.STRING, (short)1);
  private static final libthrift091.protocol.TField TOPIC_TALOS_RESOURCE_NAME_FIELD_DESC = new libthrift091.protocol.TField("topicTalosResourceName", libthrift091.protocol.TType.STRUCT, (short)2);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new QueryWorkerRequestStandardSchemeFactory());
    schemes.put(TupleScheme.class, new QueryWorkerRequestTupleSchemeFactory());
  }

  /**
   * consumer group name
   * 
   */
  public String consumerGroup; // required
  /**
   * topicTalosResourceName
   * 
   */
  public com.xiaomi.infra.galaxy.talos.thrift.TopicTalosResourceName topicTalosResourceName; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements libthrift091.TFieldIdEnum {
    /**
     * consumer group name
     * 
     */
    CONSUMER_GROUP((short)1, "consumerGroup"),
    /**
     * topicTalosResourceName
     * 
     */
    TOPIC_TALOS_RESOURCE_NAME((short)2, "topicTalosResourceName");

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
        case 1: // CONSUMER_GROUP
          return CONSUMER_GROUP;
        case 2: // TOPIC_TALOS_RESOURCE_NAME
          return TOPIC_TALOS_RESOURCE_NAME;
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
    tmpMap.put(_Fields.CONSUMER_GROUP, new libthrift091.meta_data.FieldMetaData("consumerGroup", libthrift091.TFieldRequirementType.REQUIRED, 
        new libthrift091.meta_data.FieldValueMetaData(libthrift091.protocol.TType.STRING)));
    tmpMap.put(_Fields.TOPIC_TALOS_RESOURCE_NAME, new libthrift091.meta_data.FieldMetaData("topicTalosResourceName", libthrift091.TFieldRequirementType.REQUIRED, 
        new libthrift091.meta_data.StructMetaData(libthrift091.protocol.TType.STRUCT, com.xiaomi.infra.galaxy.talos.thrift.TopicTalosResourceName.class)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    libthrift091.meta_data.FieldMetaData.addStructMetaDataMap(QueryWorkerRequest.class, metaDataMap);
  }

  public QueryWorkerRequest() {
  }

  public QueryWorkerRequest(
    String consumerGroup,
    com.xiaomi.infra.galaxy.talos.thrift.TopicTalosResourceName topicTalosResourceName)
  {
    this();
    this.consumerGroup = consumerGroup;
    this.topicTalosResourceName = topicTalosResourceName;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public QueryWorkerRequest(QueryWorkerRequest other) {
    if (other.isSetConsumerGroup()) {
      this.consumerGroup = other.consumerGroup;
    }
    if (other.isSetTopicTalosResourceName()) {
      this.topicTalosResourceName = new com.xiaomi.infra.galaxy.talos.thrift.TopicTalosResourceName(other.topicTalosResourceName);
    }
  }

  public QueryWorkerRequest deepCopy() {
    return new QueryWorkerRequest(this);
  }

  @Override
  public void clear() {
    this.consumerGroup = null;
    this.topicTalosResourceName = null;
  }

  /**
   * consumer group name
   * 
   */
  public String getConsumerGroup() {
    return this.consumerGroup;
  }

  /**
   * consumer group name
   * 
   */
  public QueryWorkerRequest setConsumerGroup(String consumerGroup) {
    this.consumerGroup = consumerGroup;
    return this;
  }

  public void unsetConsumerGroup() {
    this.consumerGroup = null;
  }

  /** Returns true if field consumerGroup is set (has been assigned a value) and false otherwise */
  public boolean isSetConsumerGroup() {
    return this.consumerGroup != null;
  }

  public void setConsumerGroupIsSet(boolean value) {
    if (!value) {
      this.consumerGroup = null;
    }
  }

  /**
   * topicTalosResourceName
   * 
   */
  public com.xiaomi.infra.galaxy.talos.thrift.TopicTalosResourceName getTopicTalosResourceName() {
    return this.topicTalosResourceName;
  }

  /**
   * topicTalosResourceName
   * 
   */
  public QueryWorkerRequest setTopicTalosResourceName(com.xiaomi.infra.galaxy.talos.thrift.TopicTalosResourceName topicTalosResourceName) {
    this.topicTalosResourceName = topicTalosResourceName;
    return this;
  }

  public void unsetTopicTalosResourceName() {
    this.topicTalosResourceName = null;
  }

  /** Returns true if field topicTalosResourceName is set (has been assigned a value) and false otherwise */
  public boolean isSetTopicTalosResourceName() {
    return this.topicTalosResourceName != null;
  }

  public void setTopicTalosResourceNameIsSet(boolean value) {
    if (!value) {
      this.topicTalosResourceName = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case CONSUMER_GROUP:
      if (value == null) {
        unsetConsumerGroup();
      } else {
        setConsumerGroup((String)value);
      }
      break;

    case TOPIC_TALOS_RESOURCE_NAME:
      if (value == null) {
        unsetTopicTalosResourceName();
      } else {
        setTopicTalosResourceName((com.xiaomi.infra.galaxy.talos.thrift.TopicTalosResourceName)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case CONSUMER_GROUP:
      return getConsumerGroup();

    case TOPIC_TALOS_RESOURCE_NAME:
      return getTopicTalosResourceName();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case CONSUMER_GROUP:
      return isSetConsumerGroup();
    case TOPIC_TALOS_RESOURCE_NAME:
      return isSetTopicTalosResourceName();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof QueryWorkerRequest)
      return this.equals((QueryWorkerRequest)that);
    return false;
  }

  public boolean equals(QueryWorkerRequest that) {
    if (that == null)
      return false;

    boolean this_present_consumerGroup = true && this.isSetConsumerGroup();
    boolean that_present_consumerGroup = true && that.isSetConsumerGroup();
    if (this_present_consumerGroup || that_present_consumerGroup) {
      if (!(this_present_consumerGroup && that_present_consumerGroup))
        return false;
      if (!this.consumerGroup.equals(that.consumerGroup))
        return false;
    }

    boolean this_present_topicTalosResourceName = true && this.isSetTopicTalosResourceName();
    boolean that_present_topicTalosResourceName = true && that.isSetTopicTalosResourceName();
    if (this_present_topicTalosResourceName || that_present_topicTalosResourceName) {
      if (!(this_present_topicTalosResourceName && that_present_topicTalosResourceName))
        return false;
      if (!this.topicTalosResourceName.equals(that.topicTalosResourceName))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_consumerGroup = true && (isSetConsumerGroup());
    list.add(present_consumerGroup);
    if (present_consumerGroup)
      list.add(consumerGroup);

    boolean present_topicTalosResourceName = true && (isSetTopicTalosResourceName());
    list.add(present_topicTalosResourceName);
    if (present_topicTalosResourceName)
      list.add(topicTalosResourceName);

    return list.hashCode();
  }

  @Override
  public int compareTo(QueryWorkerRequest other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetConsumerGroup()).compareTo(other.isSetConsumerGroup());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetConsumerGroup()) {
      lastComparison = libthrift091.TBaseHelper.compareTo(this.consumerGroup, other.consumerGroup);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetTopicTalosResourceName()).compareTo(other.isSetTopicTalosResourceName());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetTopicTalosResourceName()) {
      lastComparison = libthrift091.TBaseHelper.compareTo(this.topicTalosResourceName, other.topicTalosResourceName);
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
    StringBuilder sb = new StringBuilder("QueryWorkerRequest(");
    boolean first = true;

    sb.append("consumerGroup:");
    if (this.consumerGroup == null) {
      sb.append("null");
    } else {
      sb.append(this.consumerGroup);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("topicTalosResourceName:");
    if (this.topicTalosResourceName == null) {
      sb.append("null");
    } else {
      sb.append(this.topicTalosResourceName);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws libthrift091.TException {
    // check for required fields
    if (consumerGroup == null) {
      throw new libthrift091.protocol.TProtocolException("Required field 'consumerGroup' was not present! Struct: " + toString());
    }
    if (topicTalosResourceName == null) {
      throw new libthrift091.protocol.TProtocolException("Required field 'topicTalosResourceName' was not present! Struct: " + toString());
    }
    // check for sub-struct validity
    if (topicTalosResourceName != null) {
      topicTalosResourceName.validate();
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

  private static class QueryWorkerRequestStandardSchemeFactory implements SchemeFactory {
    public QueryWorkerRequestStandardScheme getScheme() {
      return new QueryWorkerRequestStandardScheme();
    }
  }

  private static class QueryWorkerRequestStandardScheme extends StandardScheme<QueryWorkerRequest> {

    public void read(libthrift091.protocol.TProtocol iprot, QueryWorkerRequest struct) throws libthrift091.TException {
      libthrift091.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == libthrift091.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // CONSUMER_GROUP
            if (schemeField.type == libthrift091.protocol.TType.STRING) {
              struct.consumerGroup = iprot.readString();
              struct.setConsumerGroupIsSet(true);
            } else { 
              libthrift091.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // TOPIC_TALOS_RESOURCE_NAME
            if (schemeField.type == libthrift091.protocol.TType.STRUCT) {
              struct.topicTalosResourceName = new com.xiaomi.infra.galaxy.talos.thrift.TopicTalosResourceName();
              struct.topicTalosResourceName.read(iprot);
              struct.setTopicTalosResourceNameIsSet(true);
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

    public void write(libthrift091.protocol.TProtocol oprot, QueryWorkerRequest struct) throws libthrift091.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.consumerGroup != null) {
        oprot.writeFieldBegin(CONSUMER_GROUP_FIELD_DESC);
        oprot.writeString(struct.consumerGroup);
        oprot.writeFieldEnd();
      }
      if (struct.topicTalosResourceName != null) {
        oprot.writeFieldBegin(TOPIC_TALOS_RESOURCE_NAME_FIELD_DESC);
        struct.topicTalosResourceName.write(oprot);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class QueryWorkerRequestTupleSchemeFactory implements SchemeFactory {
    public QueryWorkerRequestTupleScheme getScheme() {
      return new QueryWorkerRequestTupleScheme();
    }
  }

  private static class QueryWorkerRequestTupleScheme extends TupleScheme<QueryWorkerRequest> {

    @Override
    public void write(libthrift091.protocol.TProtocol prot, QueryWorkerRequest struct) throws libthrift091.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      oprot.writeString(struct.consumerGroup);
      struct.topicTalosResourceName.write(oprot);
    }

    @Override
    public void read(libthrift091.protocol.TProtocol prot, QueryWorkerRequest struct) throws libthrift091.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      struct.consumerGroup = iprot.readString();
      struct.setConsumerGroupIsSet(true);
      struct.topicTalosResourceName = new com.xiaomi.infra.galaxy.talos.thrift.TopicTalosResourceName();
      struct.topicTalosResourceName.read(iprot);
      struct.setTopicTalosResourceNameIsSet(true);
    }
  }

}

