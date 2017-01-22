/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: yongxing@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.client;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import com.google.common.base.Preconditions;
import libthrift091.TBase;
import libthrift091.TDeserializer;
import libthrift091.TException;
import libthrift091.TSerializer;
import libthrift091.protocol.TCompactProtocol;

import com.xiaomi.infra.galaxy.rpc.thrift.Credential;
import com.xiaomi.infra.galaxy.talos.thrift.AddSubResourceNameRequest;
import com.xiaomi.infra.galaxy.talos.thrift.ErrorCode;
import com.xiaomi.infra.galaxy.talos.thrift.GalaxyTalosException;
import com.xiaomi.infra.galaxy.talos.thrift.Message;
import com.xiaomi.infra.galaxy.talos.thrift.MessageOffset;
import com.xiaomi.infra.galaxy.talos.thrift.MessageType;
import com.xiaomi.infra.galaxy.talos.thrift.TopicAndPartition;

import static com.xiaomi.infra.galaxy.talos.client.Constants.TALOS_CLOUD_AK_PREFIX;
import static com.xiaomi.infra.galaxy.talos.client.Constants.TALOS_CLOUD_ORG_PREFIX;
import static com.xiaomi.infra.galaxy.talos.client.Constants.TALOS_CLOUD_TEAM_PREFIX;
import static com.xiaomi.infra.galaxy.talos.client.Constants.TALOS_IDENTIFIER_DELIMITER;
import static com.xiaomi.infra.galaxy.talos.client.Constants.TALOS_NAME_REGEX;
import static com.xiaomi.infra.galaxy.talos.client.Constants.TALOS_CLOUD_TOPIC_NAME_DELIMITER;

public class Utils {
  /**
   * The format of valid resource name is: ownerId#topicName#UUID
   * Note the 'ownerId' may contains the symbol '#',
   * so return topicName parsing from the tail of resourceName.
   *
   * @param topicTalosResourceName
   * @return
   */
  public static String getTopicNameByResourceName(String topicTalosResourceName) {
    String[] itemList = topicTalosResourceName.split(TALOS_IDENTIFIER_DELIMITER);
    Preconditions.checkArgument(itemList.length >= 3);
    return itemList[itemList.length - 2];
  }

  public static void checkParameterRange(String parameter, int value,
      int minValue, int maxValue) {
    if (value < minValue || value > maxValue) {
      throw new IllegalArgumentException(parameter + " should be in range [" +
          minValue + ", " + maxValue + "], got: " + value);
    }
  }

  public static void checkStartOffsetValidity(long startOffset) {
    if (startOffset >= 0 || startOffset == MessageOffset.START_OFFSET.getValue() ||
        startOffset == MessageOffset.LATEST_OFFSET.getValue()) {
      return;
    }
    throw new IllegalArgumentException("invalid startOffset: " + startOffset +
        ". It must be greater than or equal to 0, " +
        "or equal to MessageOffset.START_OFFSET/MessageOffset.LATEST_OFFSET");
  }

  public static String generateClientId() {
    return System.currentTimeMillis() + UUID.randomUUID().toString().substring(0, 8);
  }

  public static void checkTopicAndPartition(TopicAndPartition topicAndPartition) {
    if (topicAndPartition.getTopicName().contains(TALOS_CLOUD_TOPIC_NAME_DELIMITER)) {
      throw new IllegalArgumentException(
          "The topic name format in TopicAndPartition should not be: orgId/topicName");
    }
  }

  // The format of cloud topicName is: orgId/topicName
  public static void checkCloudTopicNameValidity(String topicName) {
    if (topicName == null || topicName.length() == 0) {
      throw new IllegalArgumentException("Got null topicName");
    }

    String[] items = topicName.split(TALOS_CLOUD_TOPIC_NAME_DELIMITER);
    // either 'xxx/xxx/'(split 2), '/xxx'(split 2) or 'xx//xx'(split 3) are invalid
    if (items.length != 2 || topicName.endsWith(TALOS_CLOUD_TOPIC_NAME_DELIMITER)
        || !topicName.startsWith(TALOS_CLOUD_ORG_PREFIX)) {
      throw new IllegalArgumentException(
          "The format of topicName used by cloud-manager must be: orgId/topicName");
    }

    // check real topic name validity
    checkNameValidity(items[1]);
  }

  public static void checkNameValidity(String str) {
    if (str == null || str.length() <= 0) {
      return;
    }
    if (!Pattern.matches(TALOS_NAME_REGEX, str) || str.length() > 80) {
      throw new IllegalArgumentException("invalid str: " + str +
          ". please name the str only with the regex set: [a-zA-Z0-9_-]" +
          ". Its length must be [1, 80] and cannot start with '_' or '-'.");
    }
  }

  public static String generateClientId(String prefix) {
    checkNameValidity(prefix);
    return prefix + generateClientId();
  }

  public static String generateRequestSequenceId(String clientId,
      AtomicLong requestId) {
    checkNameValidity(clientId);
    return clientId + Constants.TALOS_IDENTIFIER_DELIMITER +
        requestId.getAndIncrement();
  }

  private static ErrorCode getErrorCode(Throwable throwable) {
    if (throwable instanceof GalaxyTalosException) {
      GalaxyTalosException e = (GalaxyTalosException) throwable;
      return e.getErrorCode();
    }
    return null;
  }

  public static boolean isTopicNotExist(Throwable throwable) {
    return getErrorCode(throwable) == ErrorCode.TOPIC_NOT_EXIST;
  }

  public static boolean isPartitionNotServing(Throwable throwable) {
    return getErrorCode(throwable) == ErrorCode.PARTITION_NOT_SERVING;
  }

  public static boolean isOffsetOutOfRange(Throwable throwable) {
    return getErrorCode(throwable) == ErrorCode.MESSAGE_OFFSET_OUT_OF_RANGE;
  }

  public static void updateMessage(Message message, MessageType messageType) {
    if (!message.isSetCreateTimestamp()) {
      message.setCreateTimestamp(System.currentTimeMillis());
    }

    message.setMessageType(messageType);
  }

  public static void checkMessageValidity(Message message) {
    checkMessageLenValidity(message);
    checkMessageSequenceNumberValidity(message);
    checkMessageTypeValidity(message);
  }

  private static void checkMessageLenValidity(Message message) {
    if (!message.isSetMessage()) {
      throw new IllegalArgumentException("Field \"message\" must be set");
    }

    byte[] data = message.getMessage();
    if (data.length > Constants.TALOS_SINGLE_MESSAGE_BYTES_MAXIMAL ||
        data.length < Constants.TALOS_SINGLE_MESSAGE_BYTES_MINIMAL) {
      throw new IllegalArgumentException("Data must be less than or equal to " +
          Constants.TALOS_SINGLE_MESSAGE_BYTES_MAXIMAL + " bytes, got bytes: " +
          data.length);
    }
  }

  private static void checkMessageSequenceNumberValidity(Message message) {
    if (!message.isSetSequenceNumber()) {
      return;
    }

    String sequenceNumber = message.getSequenceNumber();
    if (sequenceNumber.length() < Constants.TALOS_PARTITION_KEY_LENGTH_MINIMAL ||
        sequenceNumber.length() > Constants.TALOS_PARTITION_KEY_LENGTH_MAXIMAL) {
      throw new IllegalArgumentException("Invalid sequenceNumber which length " +
          "must be at least " + Constants.TALOS_PARTITION_KEY_LENGTH_MINIMAL + " and at most " +
          Constants.TALOS_PARTITION_KEY_LENGTH_MAXIMAL + ", got " + sequenceNumber.length());
    }
  }

  private static void checkMessageTypeValidity(Message message) {
    if (!message.isSetMessageType()) {
      throw new IllegalArgumentException("Filed \"messageType\" must be set");
    }
  }

  public static void checkAddSubResourceNameRequest(Credential credential,
      AddSubResourceNameRequest request) {
    // check principal
    if (credential.getSecretKeyId().startsWith(TALOS_CLOUD_AK_PREFIX)) {
      throw new IllegalArgumentException(
          "Only Developer principal can add subResourceName");
    }

    // check topic
    if (request.getTopicTalosResourceName().getTopicTalosResourceName()
        .startsWith(TALOS_CLOUD_ORG_PREFIX)) {
      throw new IllegalArgumentException(
          "The topic created by cloud-manager role can not add subResourceName");
    }

    // check orgId
    if (!request.getOrgId().startsWith(TALOS_CLOUD_ORG_PREFIX)) {
      throw new IllegalArgumentException("The orgId must starts with 'CL'");
    }

    // check teamId
    if (!request.getAdminTeamId().startsWith(TALOS_CLOUD_TEAM_PREFIX)) {
      throw new IllegalArgumentException("The teamId must starts with 'CI'");
    }
  }

  public static <T extends TBase> byte[] serialize(T t) throws IOException {
    try {
      TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
      return serializer.serialize(t);
    } catch (Exception te) {
      throw new IOException("Failed to serialize thrift object: " + t, te);
    }
  }

  public static <T extends TBase> T deserialize(byte[] bytes, Class<T> clazz)
    throws IOException {
    try {
      TDeserializer deserializer = new TDeserializer(new TCompactProtocol.Factory());
      T instance = clazz.newInstance();
      deserializer.deserialize(instance, bytes);
      return instance;
    } catch (Exception te) {
      throw new IOException("Failed to deserialize thrift object", te);
    }
  }
}
