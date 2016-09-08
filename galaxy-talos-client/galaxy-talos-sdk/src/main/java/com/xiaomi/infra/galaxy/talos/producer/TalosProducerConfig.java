/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: yongxing@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.producer;

import java.util.Properties;

import com.google.common.base.Preconditions;

import com.xiaomi.infra.galaxy.talos.client.TalosClientConfig;
import com.xiaomi.infra.galaxy.talos.client.TalosClientConfigKeys;
import com.xiaomi.infra.galaxy.talos.client.Utils;
import com.xiaomi.infra.galaxy.talos.thrift.MessageCompressionType;

public class TalosProducerConfig extends TalosClientConfig {
  private int maxBufferedMsgNumber;
  private int maxBufferedMsgBytes;
  private int maxBufferedMsgTime;
  private int maxPutMsgNumber;
  private int maxPutMsgBytes;
  private int threadPoolsize;
  private int checkPartitionInterval;
  private long updatePartitionIdInterval;
  private long waitPartitionWorkingTime;
  private long updatePartitionMsgNum;
  private String compressionType;

  public TalosProducerConfig() {
    super();
    init(); // parameterChecking will be done in set function
  }

  public TalosProducerConfig(String fileName) {
    super(fileName);
    init();
    parameterChecking();
  }

  public TalosProducerConfig(Properties pro) {
    this(pro, true);
  }

  // for test
  public TalosProducerConfig(Properties pro, boolean checkParameter) {
    super(pro);
    init();
    if (checkParameter) {
      parameterChecking();
    }
  }

  private void init() {
    maxBufferedMsgNumber = Integer.parseInt(properties.getProperty(
        TalosClientConfigKeys.GALAXY_TALOS_PRODUCER_MAX_BUFFERED_MESSAGE_NUMBER,
        String.valueOf(TalosClientConfigKeys.GALAXY_TALOS_PRODUCER_MAX_BUFFERED_MESSAGE_NUMBER_DEFAULT)));
    maxBufferedMsgBytes = Integer.parseInt(properties.getProperty(
        TalosClientConfigKeys.GALAXY_TALOS_PRODUCER_MAX_BUFFERED_MESSAGE_BYTES,
        String.valueOf(TalosClientConfigKeys.GALAXY_TALOS_PRODUCER_MAX_BUFFERED_MESSAGE_BYTES_DEFAULT)));
    maxBufferedMsgTime = Integer.parseInt(properties.getProperty(
        TalosClientConfigKeys.GALAXY_TALOS_PRODUCER_MAX_BUFFERED_MILLI_SECS,
        String.valueOf(TalosClientConfigKeys.GALAXY_TALOS_PRODUCER_MAX_BUFFERED_MILLI_SECS_DEFAULT)));
    maxPutMsgNumber = Integer.parseInt(properties.getProperty(
        TalosClientConfigKeys.GALAXY_TALOS_PRODUCER_MAX_PUT_MESSAGE_NUMBER,
        String.valueOf(TalosClientConfigKeys.GALAXY_TALOS_PRODUCER_MAX_PUT_MESSAGE_NUMBER_DEFAULT)));
    maxPutMsgBytes = Integer.parseInt(properties.getProperty(
        TalosClientConfigKeys.GALAXY_TALOS_PRODUCER_MAX_PUT_MESSAGE_BYTES,
        String.valueOf(TalosClientConfigKeys.GALAXY_TALOS_PRODUCER_MAX_PUT_MESSAGE_BYTES_DEFAULT)));
    threadPoolsize = Integer.parseInt(properties.getProperty(
        TalosClientConfigKeys.GALAXY_TALOS_PRODUCER_THREAD_POOL_SIZE,
        String.valueOf(TalosClientConfigKeys.GALAXY_TALOS_PRODUCER_THREAD_POOL_SIZE_DEFAULT)));
    checkPartitionInterval = Integer.parseInt(properties.getProperty(
        TalosClientConfigKeys.GALAXY_TALOS_PRODUCER_CHECK_PARTITION_INTERVAL,
        String.valueOf(TalosClientConfigKeys.GALAXY_TALOS_PRODUCER_CHECK_PARTITION_INTERVAL_DEFAULT)));
    updatePartitionIdInterval = Long.parseLong(properties.getProperty(
        TalosClientConfigKeys.GALAXY_TALOS_PRODUCER_UPDATE_PARTITIONID_INTERVAL,
        String.valueOf(TalosClientConfigKeys.GALAXY_TALOS_PRODUCER_UPDATE_PARTITIONID_INTERVAL_DEFAULT)));
    waitPartitionWorkingTime = Long.parseLong(properties.getProperty(
        TalosClientConfigKeys.GALAXY_TALOS_PRODUCER_WAIT_PARTITION_WORKING_TIME,
        String.valueOf(TalosClientConfigKeys.GALAXY_TALOS_PRODUCER_WAIT_PARTITION_WORKING_TIME_DEFAULT)));
    updatePartitionMsgNum = Long.parseLong(properties.getProperty(
        TalosClientConfigKeys.GALAXY_TALOS_PRODUCER_UPDATE_PARTITION_MSGNUMBER,
        String.valueOf(TalosClientConfigKeys.GALAXY_TALOS_PRODUCER_UPDATE_PARTITION_MSGNUMBER_DEFAULT)));
    compressionType = properties.getProperty(
        TalosClientConfigKeys.GALAXY_TALOS_PRODUCER_COMPRESSION_TYPE,
        TalosClientConfigKeys.GALAXY_TALOS_PRODUCER_COMPRESSION_TYPE_DEFAULT);

    if (!compressionType.equals("NONE") && !compressionType.equals("SNAPPY")
        && !compressionType.equals("GZIP")) {
      throw new RuntimeException("Unsupported Compression Type: " + compressionType);
    }
  }

  private void parameterChecking() {
    Utils.checkParameterRange(
        TalosClientConfigKeys.GALAXY_TALOS_PRODUCER_MAX_PUT_MESSAGE_NUMBER,
        maxPutMsgNumber,
        TalosClientConfigKeys.GALAXY_TALOS_PRODUCER_MAX_PUT_MESSAGE_NUMBER_MINIMUM,
        TalosClientConfigKeys.GALAXY_TALOS_PRODUCER_MAX_PUT_MESSAGE_NUMBER_MAXIMUM);
    Utils.checkParameterRange(
        TalosClientConfigKeys.GALAXY_TALOS_PRODUCER_MAX_PUT_MESSAGE_BYTES,
        maxPutMsgBytes,
        TalosClientConfigKeys.GALAXY_TALOS_PRODUCER_MAX_PUT_MESSAGE_BYTES_MINIMUM,
        TalosClientConfigKeys.GALAXY_TALOS_PRODUCER_MAX_PUT_MESSAGE_BYTES_MAXIMUM);
    Utils.checkParameterRange(
        TalosClientConfigKeys.GALAXY_TALOS_PRODUCER_CHECK_PARTITION_INTERVAL,
        checkPartitionInterval,
        TalosClientConfigKeys.GALAXY_TALOS_PRODUCER_CHECK_PARTITION_INTERVAL_MINIMUM,
        TalosClientConfigKeys.GALAXY_TALOS_PRODUCER_CHECK_PARTITION_INTERVAL_MAXIMUM);
    Utils.checkParameterRange(
        TalosClientConfigKeys.GALAXY_TALOS_PRODUCER_UPDATE_PARTITIONID_INTERVAL,
        (int) updatePartitionIdInterval,
        (int) TalosClientConfigKeys.GALAXY_TALOS_PRODUCER_UPDATE_PARTITIONID_INTERVAL_MINIMUM,
        (int) TalosClientConfigKeys.GALAXY_TALOS_PRODUCER_UPDATE_PARTITIONID_INTERVAL_MAXIMUM);
  }

  public int getMaxBufferedMsgNumber() {
    return maxBufferedMsgNumber;
  }

  public int getMaxBufferedMsgBytes() {
    return maxBufferedMsgBytes;
  }

  public int getMaxBufferedMsgTime() {
    return maxBufferedMsgTime;
  }

  public int getMaxPutMsgNumber() {
    return maxPutMsgNumber;
  }

  public int getMaxPutMsgBytes() {
    return maxPutMsgBytes;
  }

  public int getThreadPoolsize() {
    return threadPoolsize;
  }

  public int getCheckPartitionInterval() {
    return checkPartitionInterval;
  }

  public long getUpdatePartitionIdInterval() {
    return updatePartitionIdInterval;
  }

  public long getWaitPartitionWorkingTime() {
    return waitPartitionWorkingTime;
  }

  public long getUpdatePartitionMsgNum() {
    return updatePartitionMsgNum;
  }

  public MessageCompressionType getCompressionType() {
    if (compressionType.equals("NONE")) {
      return MessageCompressionType.NONE;
    } else if (compressionType.equals("SNAPPY")) {
      return MessageCompressionType.SNAPPY;
    } else {
      Preconditions.checkArgument(compressionType.equals("GZIP"));
      return MessageCompressionType.GZIP;
    }
  }

  public void setMaxBufferedMsgNumber(int maxBufferedMsgNumber) {
    this.maxBufferedMsgNumber = maxBufferedMsgNumber;
  }

  public void setMaxBufferedMsgBytes(int maxBufferedMsgBytes) {
    this.maxBufferedMsgBytes = maxBufferedMsgBytes;
  }

  public void setMaxBufferedMsgTime(int maxBufferedMsgTime) {
    this.maxBufferedMsgTime = maxBufferedMsgTime;
  }

  public void setMaxPutMsgNumber(int maxPutMsgNumber) {
    this.maxPutMsgNumber = maxPutMsgNumber;
    Utils.checkParameterRange(
        TalosClientConfigKeys.GALAXY_TALOS_PRODUCER_MAX_PUT_MESSAGE_NUMBER,
        this.maxPutMsgNumber,
        TalosClientConfigKeys.GALAXY_TALOS_PRODUCER_MAX_PUT_MESSAGE_NUMBER_MINIMUM,
        TalosClientConfigKeys.GALAXY_TALOS_PRODUCER_MAX_PUT_MESSAGE_NUMBER_MAXIMUM);
  }

  public void setMaxPutMsgBytes(int maxPutMsgBytes) {
    this.maxPutMsgBytes = maxPutMsgBytes;
    Utils.checkParameterRange(
        TalosClientConfigKeys.GALAXY_TALOS_PRODUCER_MAX_PUT_MESSAGE_BYTES,
        this.maxPutMsgBytes,
        TalosClientConfigKeys.GALAXY_TALOS_PRODUCER_MAX_PUT_MESSAGE_BYTES_MINIMUM,
        TalosClientConfigKeys.GALAXY_TALOS_PRODUCER_MAX_PUT_MESSAGE_BYTES_MAXIMUM);
  }

  public void setThreadPoolsize(int threadPoolsize) {
    this.threadPoolsize = threadPoolsize;
  }

  public void setCheckPartitionInterval(int checkPartitionInterval) {
    this.checkPartitionInterval = checkPartitionInterval;
    Utils.checkParameterRange(
        TalosClientConfigKeys.GALAXY_TALOS_PRODUCER_CHECK_PARTITION_INTERVAL,
        this.checkPartitionInterval,
        TalosClientConfigKeys.GALAXY_TALOS_PRODUCER_CHECK_PARTITION_INTERVAL_MINIMUM,
        TalosClientConfigKeys.GALAXY_TALOS_PRODUCER_CHECK_PARTITION_INTERVAL_MAXIMUM);
  }

  public void setUpdatePartitionIdInterval(long updatePartitionIdInterval) {
    this.updatePartitionIdInterval = updatePartitionIdInterval;
    Utils.checkParameterRange(
        TalosClientConfigKeys.GALAXY_TALOS_PRODUCER_UPDATE_PARTITIONID_INTERVAL,
        (int) updatePartitionIdInterval,
        (int) TalosClientConfigKeys.GALAXY_TALOS_PRODUCER_UPDATE_PARTITIONID_INTERVAL_MINIMUM,
        (int) TalosClientConfigKeys.GALAXY_TALOS_PRODUCER_UPDATE_PARTITIONID_INTERVAL_MAXIMUM);
  }

  public void setWaitPartitionWorkingTime(long waitPartitionWorkingTime) {
    this.waitPartitionWorkingTime = waitPartitionWorkingTime;
  }

  public void setUpdatePartitionMsgNum(long updatePartitionMsgNum) {
    this.updatePartitionMsgNum = updatePartitionMsgNum;
  }

  public void setCompressionType(String compressionType) {
    this.compressionType = compressionType;
  }
}
