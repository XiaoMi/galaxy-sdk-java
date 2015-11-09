/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: yongxing@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.producer;

import org.apache.hadoop.conf.Configuration;

import com.xiaomi.infra.galaxy.talos.client.TalosClientConfig;
import com.xiaomi.infra.galaxy.talos.client.TalosClientConfigKeys;

public class TalosProducerConfig extends TalosClientConfig {
  private int maxBufferedMsgNumber;
  private int maxBufferedMsgBytes;
  private int maxBufferedMsgTime;
  private int scanPartitionQueueInterval;
  private int maxPutMsgNumber;
  private int maxPutMsgBytes;
  private int threadPoolsize;
  private int checkPartitionInterval;
  private long updatePartitionIdInterval;

  public TalosProducerConfig(Configuration configuration) {
    super(configuration);
    maxBufferedMsgNumber = configuration.getInt(
        TalosClientConfigKeys.GALAXY_TALOS_PRODUCER_MAX_BUFFERED_MESSAGE_NUMBER,
        TalosClientConfigKeys.GALAXY_TALOS_PRODUCER_MAX_BUFFERED_MESSAGE_NUMBER_DEFAULT);
    maxBufferedMsgBytes = configuration.getInt(
        TalosClientConfigKeys.GALAXY_TALOS_PRODUCER_MAX_BUFFERED_MESSAGE_BYTES,
        TalosClientConfigKeys.GALAXY_TALOS_PRODUCER_MAX_BUFFERED_MESSAGE_BYTES_DEFAULT);
    maxBufferedMsgTime = configuration.getInt(
        TalosClientConfigKeys.GALAXY_TALOS_PRODUCER_MAX_BUFFERED_MILLI_SECS,
        TalosClientConfigKeys.GALAXY_TALOS_PRODUCER_MAX_BUFFERED_MILLI_SECS_DEFAULT);
    scanPartitionQueueInterval = configuration.getInt(
        TalosClientConfigKeys.GALAXY_TALOS_PRODUCER_SCAN_PARTITION_QUEUE_INTERVAL,
        TalosClientConfigKeys.GALAXY_TALOS_PRODUCER_SCAN_PARTITION_QUEUE_INTERVAL_DEFAULT);
    maxPutMsgNumber = configuration.getInt(
        TalosClientConfigKeys.GALAXY_TALOS_PRODUCER_MAX_PUT_MESSAGE_NUMBER,
        TalosClientConfigKeys.GALAXY_TALOS_PRODUCER_MAX_PUT_MESSAGE_NUMBER_DEFAULT);
    maxPutMsgBytes = configuration.getInt(
        TalosClientConfigKeys.GALAXY_TALOS_PRODUCER_MAX_PUT_MESSAGE_BYTES,
        TalosClientConfigKeys.GALAXY_TALOS_PRODUCER_MAX_PUT_MESSAGE_BYTES_DEFAULT);
    threadPoolsize = configuration.getInt(
        TalosClientConfigKeys.GALAXY_TALOS_PRODUCER_THREAD_POOL_SIZE,
        TalosClientConfigKeys.GALAXY_TALOS_PRODUCER_THREAD_POOL_SIZE_DEFAULT);
    checkPartitionInterval = configuration.getInt(
        TalosClientConfigKeys.GALAXY_TALOS_PRODUCER_CHECK_PARTITION_INTERVAL,
        TalosClientConfigKeys.GALAXY_TALOS_PRODUCER_CHECK_PARTITION_INTERVAL_DEFAULT);
    updatePartitionIdInterval = configuration.getLong(
        TalosClientConfigKeys.GALAXY_TALOS_PRODUCER_UPDATE_PARTITIONID_INTERVAL,
        TalosClientConfigKeys.GALAXY_TALOS_PRODUCER_UPDATE_PARTITIONID_INTERVAL_DEFAULT);
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

  public int getScanPartitionQueueInterval() {
    return scanPartitionQueueInterval;
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
}
