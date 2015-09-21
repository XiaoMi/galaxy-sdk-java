/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: yongxing@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.producer;

import org.apache.hadoop.conf.Configuration;

import com.xiaomi.infra.galaxy.talos.client.TalosClientConfig;
import com.xiaomi.infra.galaxy.talos.common.config.TalosConfigKeys;

public class TalosProducerConfig extends TalosClientConfig {
  private int maxBufferedMsgNumber;
  private int maxBufferedMsgBytes;
  private int maxBufferedMsgTime;
  private int scanPartitionQueueInterval;
  private int maxPutMsgNumber;
  private int maxPutMsgBytes;
  private int threadPoolsize;
  private int checkPartitionInterval;

  public TalosProducerConfig(Configuration configuration) {
    super(configuration);
    maxBufferedMsgNumber = configuration.getInt(
        TalosConfigKeys.GALAXY_TALOS_PRODUCER_MAX_BUFFERED_MESSAGE_NUMBER,
        TalosConfigKeys.GALAXY_TALOS_PRODUCER_MAX_BUFFERED_MESSAGE_NUMBER_DEFAULT);
    maxBufferedMsgBytes = configuration.getInt(
        TalosConfigKeys.GALAXY_TALOS_PRODUCER_MAX_BUFFERED_MESSAGE_BYTES,
        TalosConfigKeys.GALAXY_TALOS_PRODUCER_MAX_BUFFERED_MESSAGE_BYTES_DEFAULT);
    maxBufferedMsgTime = configuration.getInt(
        TalosConfigKeys.GALAXY_TALOS_PRODUCER_MAX_BUFFERED_MILLI_SECS,
        TalosConfigKeys.GALAXY_TALOS_PRODUCER_MAX_BUFFERED_MILLI_SECS_DEFAULT);
    scanPartitionQueueInterval = configuration.getInt(
        TalosConfigKeys.GALAXY_TALOS_PRODUCER_SCAN_PARTITION_QUEUE_INTERVAL,
        TalosConfigKeys.GALAXY_TALOS_PRODUCER_SCAN_PARTITION_QUEUE_INTERVAL_DEFAULT);
    maxPutMsgNumber = configuration.getInt(
        TalosConfigKeys.GALAXY_TALOS_PRODUCER_MAX_PUT_MESSAGE_NUMBER,
        TalosConfigKeys.GALAXY_TALOS_PRODUCER_MAX_PUT_MESSAGE_NUMBER_DEFAULT);
    maxPutMsgBytes = configuration.getInt(
        TalosConfigKeys.GALAXY_TALOS_PRODUCER_MAX_PUT_MESSAGE_BYTES,
        TalosConfigKeys.GALAXY_TALOS_PRODUCER_MAX_PUT_MESSAGE_BYTES_DEFAULT);
    threadPoolsize = configuration.getInt(
        TalosConfigKeys.GALAXY_TALOS_PRODUCER_THREAD_POOL_SIZE,
        TalosConfigKeys.GALAXY_TALOS_PRODUCER_THREAD_POOL_SIZE_DEFAULT);
    checkPartitionInterval = configuration.getInt(
        TalosConfigKeys.GALAXY_TALOS_PRODUCER_CHECK_PARTITION_INTERVAL,
        TalosConfigKeys.GALAXY_TALOS_PRODUCER_CHECK_PARTITION_INTERVAL_DEFAULT);
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
}
