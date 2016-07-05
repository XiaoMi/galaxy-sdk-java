/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: yongxing@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.consumer;


import java.util.Properties;

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
  private boolean checkpointAutoCommit;

  public TalosConsumerConfig(String fileName) {
    super(fileName);
    init();
    parameterChecking();
  }

  public TalosConsumerConfig(Properties pro) {
    this(pro, true);
  }

  // for test
  public TalosConsumerConfig(Properties pro, boolean checkParameter) {
    super(pro);
    init();
    if (checkParameter) {
      parameterChecking();
    }
  }

  private void init() {
    partitionCheckInterval = Integer.parseInt(properties.getProperty(
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_CHECK_PARTITION_INTERVAL,
        String.valueOf(TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_CHECK_PARTITION_INTERVAL_DEFAULT)));
    workerInfoCheckInterval = Integer.parseInt(properties.getProperty(
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_CHECK_WORKER_INFO_INTERVAL,
        String.valueOf(TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_CHECK_WORKER_INFO_INTERVAL_DEFAULT)));
    reNewCheckInterval = Integer.parseInt(properties.getProperty(
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_RENEW_INTERVAL,
        String.valueOf(TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_RENEW_INTERVAL_DEFAULT)));
    reNewMaxRetry = Integer.parseInt(properties.getProperty(
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_RENEW_MAX_RETRY,
        String.valueOf(TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_RENEW_MAX_RETRY_DEFAULT)));
    fetchMessageInterval = Integer.parseInt(properties.getProperty(
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_FETCH_INTERVAL,
        String.valueOf(TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_FETCH_INTERVAL_DEFAULT)));
    maxFetchRecords = Integer.parseInt(properties.getProperty(
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_MAX_FETCH_RECORDS,
        String.valueOf(TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_MAX_FETCH_RECORDS_DEFAULT)));
    selfRegisterMaxRetry = Integer.parseInt(properties.getProperty(
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_REGISTER_MAX_RETRY,
        String.valueOf(TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_REGISTER_MAX_RETRY_DEFAULT)));
    commitOffsetThreshold = Integer.parseInt(properties.getProperty(
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_COMMIT_OFFSET_THRESHOLD,
        String.valueOf(TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_COMMIT_OFFSET_THRESHOLD_DEFAULT)));
    commitOffsetInterval = Integer.parseInt(properties.getProperty(
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_COMMIT_OFFSET_INTERVAL,
        String.valueOf(TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_COMMIT_OFFSET_INTERVAL_DEFAULT)));
    checkLastCommitOffset = Boolean.parseBoolean(properties.getProperty(
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_CHECK_LAST_COMMIT_OFFSET_SWITCH,
        String.valueOf(TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_CHECK_LAST_COMMIT_OFFSET_SWITCH_DEFAULT)));
    waitPartitionWorkingTime = Long.parseLong(properties.getProperty(
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_WAIT_PARTITION_WORKING_TIME,
        String.valueOf(TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_WAIT_PARTITION_WORKING_TIME_DEFAULT)));
    resetLatestOffset = Boolean.parseBoolean(properties.getProperty(
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_RESET_LATEST_OFFSET,
        String.valueOf(TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_RESET_LATEST_OFFSET_DEFAULT)));
    checkpointAutoCommit = Boolean.parseBoolean(properties.getProperty(
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_CHECKPOINT_AUTO_COMMIT,
        String.valueOf(TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_CHECKPOINT_AUTO_COMMIT_DEFAULT)));
  }

  private void parameterChecking() {
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
    Utils.checkParameterRange(
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_MAX_FETCH_RECORDS,
        maxFetchRecords,
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_MAX_FETCH_RECORDS_MINIMUM,
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_MAX_FETCH_RECORDS_MAXIMUM);
    Utils.checkParameterRange(
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_COMMIT_OFFSET_THRESHOLD,
        commitOffsetThreshold,
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_COMMIT_OFFSET_THRESHOLD_MINIMUM,
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_COMMIT_OFFSET_THRESHOLD_MAXIMUM);
    Utils.checkParameterRange(
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_COMMIT_OFFSET_INTERVAL,
        commitOffsetInterval,
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_COMMIT_OFFSET_INTERVAL_MINIMUM,
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_COMMIT_OFFSET_INTERVAL_MAXIMUM);
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

  public boolean isCheckpointAutoCommit() {
    return checkpointAutoCommit;
  }
}
