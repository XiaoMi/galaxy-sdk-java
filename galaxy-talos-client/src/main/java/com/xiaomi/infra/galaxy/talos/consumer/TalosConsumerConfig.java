/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: yongxing@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.consumer;

import org.apache.hadoop.conf.Configuration;

import com.xiaomi.infra.galaxy.talos.client.TalosClientConfig;
import com.xiaomi.infra.galaxy.talos.client.TalosClientConfigKeys;
import com.xiaomi.infra.galaxy.talos.client.Utils;

public class TalosConsumerConfig extends TalosClientConfig {
  private int partitionCheckInterval;
  private int workerInfoCheckInterval;
  private int reNewCheckInterval;
  private int reNewMaxRetry;
  private int maxFetchRecords;
  private int selfRegisterMaxRetry;
  private int commitOffsetThreshold;
  private int commitOffsetInterval;
  private int fetchMessageInterval;
  private boolean checkLastCommitOffset;
  private long waitPartitionWorkingTime;
  private boolean resetLatestOffset;

  public TalosConsumerConfig(Configuration configuration) {
    this(configuration, true);
  }

  public TalosConsumerConfig(Configuration configuration, boolean checkParameter) {
    super(configuration);
    partitionCheckInterval = configuration.getInt(
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_CHECK_PARTITION_INTERVAL,
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_CHECK_PARTITION_INTERVAL_DEFAULT);
    workerInfoCheckInterval = configuration.getInt(
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_CHECK_WORKER_INFO_INTERVAL,
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_CHECK_WORKER_INFO_INTERVAL_DEFAULT);
    reNewCheckInterval = configuration.getInt(
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_RENEW_INTERVAL,
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_RENEW_INTERVAL_DEFAULT);
    reNewMaxRetry = configuration.getInt(
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_RENEW_MAX_RETRY,
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_RENEW_MAX_RETRY_DEFAULT);
    fetchMessageInterval = configuration.getInt(
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_FETCH_INTERVAL,
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_FETCH_INTERVAL_DEFAULT);
    maxFetchRecords = configuration.getInt(
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_MAX_FETCH_RECORDS,
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_MAX_FETCH_RECORDS_DEFAULT);
    selfRegisterMaxRetry = configuration.getInt(
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_REGISTER_MAX_RETRY,
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_REGISTER_MAX_RETRY_DEFAULT);
    commitOffsetThreshold = configuration.getInt(
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_COMMIT_OFFSET_THRESHOLD,
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_COMMIT_OFFSET_THRESHOLD_DEFAULT);
    commitOffsetInterval = configuration.getInt(
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_COMMIT_OFFSET_INTERVAL,
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_COMMIT_OFFSET_INTERVAL_DEFAULT);
    checkLastCommitOffset = configuration.getBoolean(
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_CHECK_LAST_COMMIT_OFFSET_SWITCH,
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_CHECK_LAST_COMMIT_OFFSET_SWITCH_DEFAULT);
    waitPartitionWorkingTime = configuration.getLong(
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_WAIT_PARTITION_WORKING_TIME,
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_WAIT_PARTITION_WORKING_TIME_DEFAULT);
    resetLatestOffset = configuration.getBoolean(
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_RESET_LATEST_OFFSET,
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_RESET_LATEST_OFFSET_DEFAULT);

    if (checkParameter) {
      Utils.checkParameterRange(
          TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_CHECK_PARTITION_INTERVAL,
          partitionCheckInterval,
          TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_CHECK_PARTITION_INTERVAL_MINIMUM,
          TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_CHECK_PARTITION_INTERVAL_MAXIMUM);
      Utils.checkParameterRange(
          TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_CHECK_WORKER_INFO_INTERVAL,
          workerInfoCheckInterval,
          TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_CHECK_WORKER_INFO_INTERVAL_MINIMUM,
          TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_CHECK_WORKER_INFO_INTERVAL_MAXIMUM);
      Utils.checkParameterRange(
          TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_RENEW_INTERVAL,
          reNewCheckInterval,
          TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_RENEW_INTERVAL_MINIMUM,
          TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_RENEW_INTERVAL_MAXIMUM);
      Utils.checkParameterRange(
          TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_RENEW_MAX_RETRY,
          reNewMaxRetry,
          TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_RENEW_MAX_RETRY_MINIMUM,
          TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_RENEW_MAX_RETRY_MAXIMUM);
      Utils.checkParameterRange(
          TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_FETCH_INTERVAL,
          fetchMessageInterval,
          TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_FETCH_INTERVAL_MINIMUM,
          TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_FETCH_INTERVAL_MAXIMUM);
    }
  }

  public int getPartitionCheckInterval() {
    return partitionCheckInterval;
  }

  public int getWorkerInfoCheckInterval() {
    return workerInfoCheckInterval;
  }

  public int getReNewCheckInterval() {
    return reNewCheckInterval;
  }

  public int getReNewMaxRetry() {
    return reNewMaxRetry;
  }

  public int getMaxFetchRecords() {
    return maxFetchRecords;
  }

  public int getSelfRegisterMaxRetry() {
    return selfRegisterMaxRetry;
  }

  public int getCommitOffsetThreshold() {
    return commitOffsetThreshold;
  }

  public int getCommitOffsetInterval() {
    return commitOffsetInterval;
  }

  public int getFetchMessageInterval() {
    return fetchMessageInterval;
  }

  public boolean isCheckLastCommitOffset() {
    return checkLastCommitOffset;
  }

  public long getWaitPartitionWorkingTime() {
    return waitPartitionWorkingTime;
  }

  public boolean isResetLatestOffset() {
    return resetLatestOffset;
  }
}
