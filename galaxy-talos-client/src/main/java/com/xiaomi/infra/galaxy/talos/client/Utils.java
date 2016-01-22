/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: yongxing@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.client;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Preconditions;

import com.xiaomi.infra.galaxy.talos.thrift.ErrorCode;
import com.xiaomi.infra.galaxy.talos.thrift.GalaxyTalosException;

import static com.xiaomi.infra.galaxy.talos.client.Constants.TALOS_IDENTIFIER_DELIMITER;

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

  public static String generateClientId() {
    return System.currentTimeMillis() + UUID.randomUUID().toString().substring(0, 8);
  }

  // TODO: check prefix can not contains character '#'
  public static String generateClientId(String prefix) {
    return prefix + generateClientId();
  }

  public static String generateRequestSequenceId(String clientId,
      AtomicLong requestId) {
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
}
