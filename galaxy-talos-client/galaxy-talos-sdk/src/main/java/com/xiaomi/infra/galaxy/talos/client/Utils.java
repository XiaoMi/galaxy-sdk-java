/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: yongxing@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.client;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import com.google.common.base.Preconditions;

import com.xiaomi.infra.galaxy.talos.thrift.ErrorCode;
import com.xiaomi.infra.galaxy.talos.thrift.GalaxyTalosException;
import com.xiaomi.infra.galaxy.talos.thrift.MessageOffset;

import static com.xiaomi.infra.galaxy.talos.client.Constants.TALOS_IDENTIFIER_DELIMITER;
import static com.xiaomi.infra.galaxy.talos.client.Constants.TALOS_NAME_REGEX;

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

  public static void checkNameValidity(String str) {
    if (!Pattern.matches(TALOS_NAME_REGEX, str) || str == null ||
        str.length() <= 0 || str.length() > 80) {
      throw new IllegalArgumentException("invalid str: " + str +
          ". please name the str only with the regex set: [a-zA-Z0-9_]" +
          ". And the str length must be [1, 80]");
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

  public static void checkMessageLenValidity(byte[] data) {
    Preconditions.checkNotNull(data);
    if (data.length > Constants.TALOS_SINGLE_MESSAGE_BYTES_MAXIMAL ||
        data.length < Constants.TALOS_SINGLE_MESSAGE_BYTES_MINIMAL) {
      throw new IllegalArgumentException("Data must be less than or equal to " +
          Constants.TALOS_SINGLE_MESSAGE_BYTES_MAXIMAL + " bytes, got bytes: " +
          data.length);
    }
  }
}
