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
  private boolean resetLatestOffsetWhenOutOfRange;
  private boolean checkpointAutoCommit;
  private boolean resetOffsetWhenStart;
  private long resetOffsetValueWhenStart;
  private String localOffsetPath;
  private long checkpointInterval;
  private boolean synchronousCheckpoint;

  @Deprecated
  public TalosConsumerConfig() {
    super();
    init(); // parameterChecking will be done in set function
  }

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
    resetLatestOffsetWhenOutOfRange = Boolean.parseBoolean(properties.getProperty(
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_OUT_OF_RANGE_RESET_LATEST_OFFSET,
        String.valueOf(TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_OUT_OF_RANGE_RESET_LATEST_OFFSET_DEFAULT)));
    checkpointAutoCommit = Boolean.parseBoolean(properties.getProperty(
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_CHECKPOINT_AUTO_COMMIT,
        String.valueOf(TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_CHECKPOINT_AUTO_COMMIT_DEFAULT)));
    resetOffsetWhenStart = Boolean.parseBoolean(properties.getProperty(
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_START_WHETHER_RESET_OFFSET,
        String.valueOf(TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_START_WHETHER_RESET_OFFSET_DEFAULT)));
    resetOffsetValueWhenStart = Long.parseLong(properties.getProperty(
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_START_RESET_OFFSET_VALUE,
        String.valueOf(TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_START_RESET_OFFSET_AS_START)));
    localOffsetPath = properties.getProperty(TalosClientConfigKeys.GALAXY_TALOS_GREEDYCONSUMER_LOCAL_OFFSET_PATH,
        TalosClientConfigKeys.GALAXY_TALOS_GREEDYCONSUMER_LOCAL_OFFSET_PATH_DEFUALT);
    checkpointInterval = Integer.parseInt(properties.getProperty(
        TalosClientConfigKeys.GALAXY_TALOS_GREEDYCONSUMER_CHECKPOINT_INTERVAL,
        String.valueOf(TalosClientConfigKeys.GALAXY_TALOS_GREEDYCONSUMER_CHECKPOINT_INTERVAL_DEFAULT)));
    synchronousCheckpoint = Boolean.parseBoolean(properties.getProperty(
        TalosClientConfigKeys.GALAXY_TALOS_GREEDYCONSUMER_SYNCHRONOUS_CHECKPOINT,
        String.valueOf(TalosClientConfigKeys.GALAXY_TALOS_GREEDYCONSUMER_SYNCHRONOUS_CHECKPOINT_DEFAULT)));
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
    Utils.checkParameterRange(
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_START_RESET_OFFSET_VALUE,
        (int) resetOffsetValueWhenStart,
        (int) TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_START_RESET_OFFSET_AS_END,
        (int) TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_START_RESET_OFFSET_AS_START);
    Utils.checkParameterRange(
        TalosClientConfigKeys.GALAXY_TALOS_GREEDYCONSUMER_CHECKPOINT_INTERVAL,
        (int) checkpointInterval,
        (int) TalosClientConfigKeys.GALAXY_TALOS_GREEDYCONSUMER_CHECKPOINT_INTERVAL_MINIMUM,
        (int) TalosClientConfigKeys.GALAXY_TALOS_GREEDYCONSUMER_CHECKPOINT_INTERVAL_MAXIMUM);
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

  public boolean isResetLatestOffsetWhenOutOfRange() {
    return resetLatestOffsetWhenOutOfRange;
  }

  public boolean isCheckpointAutoCommit() {
    return checkpointAutoCommit;
  }

  public boolean isResetOffsetWhenStart() {
    return resetOffsetWhenStart;
  }

  public long getResetOffsetValueWhenStart() {
    return resetOffsetValueWhenStart;
  }

  public void setPartitionCheckInterval(int partitionCheckInterval) {
    this.partitionCheckInterval = partitionCheckInterval;
    Utils.checkParameterRange(
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_CHECK_PARTITION_INTERVAL,
        this.partitionCheckInterval,
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_CHECK_PARTITION_INTERVAL_MINIMUM,
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_CHECK_PARTITION_INTERVAL_MAXIMUM);
  }

  public String getLocalOffsetPath(){ return localOffsetPath; }

  public long getCheckpointInterval() { return checkpointInterval; }

  public boolean isSynchronousCheckpoint(){return synchronousCheckpoint; }

  public void setWorkerInfoCheckInterval(int workerInfoCheckInterval) {
    this.workerInfoCheckInterval = workerInfoCheckInterval;
    Utils.checkParameterRange(
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_CHECK_WORKER_INFO_INTERVAL,
        this.workerInfoCheckInterval,
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_CHECK_WORKER_INFO_INTERVAL_MINIMUM,
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_CHECK_WORKER_INFO_INTERVAL_MAXIMUM);
  }

  public void setReNewCheckInterval(int reNewCheckInterval) {
    this.reNewCheckInterval = reNewCheckInterval;
    Utils.checkParameterRange(
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_RENEW_INTERVAL,
        this.reNewCheckInterval,
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_RENEW_INTERVAL_MINIMUM,
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_RENEW_INTERVAL_MAXIMUM);
  }

  public void setReNewMaxRetry(int reNewMaxRetry) {
    this.reNewMaxRetry = reNewMaxRetry;
    Utils.checkParameterRange(
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_RENEW_MAX_RETRY,
        this.reNewMaxRetry,
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_RENEW_MAX_RETRY_MINIMUM,
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_RENEW_MAX_RETRY_MAXIMUM);
  }

  public void setMaxFetchRecords(int maxFetchRecords) {
    this.maxFetchRecords = maxFetchRecords;
    Utils.checkParameterRange(
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_MAX_FETCH_RECORDS,
        this.maxFetchRecords,
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_MAX_FETCH_RECORDS_MINIMUM,
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_MAX_FETCH_RECORDS_MAXIMUM);
  }

  public void setSelfRegisterMaxRetry(int selfRegisterMaxRetry) {
    this.selfRegisterMaxRetry = selfRegisterMaxRetry;
  }

  public void setCommitOffsetThreshold(int commitOffsetThreshold) {
    this.commitOffsetThreshold = commitOffsetThreshold;
    Utils.checkParameterRange(
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_COMMIT_OFFSET_THRESHOLD,
        this.commitOffsetThreshold,
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_COMMIT_OFFSET_THRESHOLD_MINIMUM,
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_COMMIT_OFFSET_THRESHOLD_MAXIMUM);
  }

  public void setCommitOffsetInterval(int commitOffsetInterval) {
    this.commitOffsetInterval = commitOffsetInterval;
    Utils.checkParameterRange(
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_COMMIT_OFFSET_INTERVAL,
        this.commitOffsetInterval,
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_COMMIT_OFFSET_INTERVAL_MINIMUM,
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_COMMIT_OFFSET_INTERVAL_MAXIMUM);
  }

  public void setFetchMessageInterval(int fetchMessageInterval) {
    this.fetchMessageInterval = fetchMessageInterval;
    Utils.checkParameterRange(
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_FETCH_INTERVAL,
        this.fetchMessageInterval,
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_FETCH_INTERVAL_MINIMUM,
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_FETCH_INTERVAL_MAXIMUM);
  }

  public void setCheckLastCommitOffset(boolean checkLastCommitOffset) {
    this.checkLastCommitOffset = checkLastCommitOffset;
  }

  public void setWaitPartitionWorkingTime(long waitPartitionWorkingTime) {
    this.waitPartitionWorkingTime = waitPartitionWorkingTime;
  }

  public void setResetLatestOffsetWhenOutOfRange(boolean resetLatestOffsetWhenOutOfRange) {
    this.resetLatestOffsetWhenOutOfRange = resetLatestOffsetWhenOutOfRange;
  }

  public void setCheckpointAutoCommit(boolean checkpointAutoCommit) {
    this.checkpointAutoCommit = checkpointAutoCommit;
  }

  public void setResetOffsetWhenStart(boolean resetOffsetWhenStart) {
    this.resetOffsetWhenStart = resetOffsetWhenStart;
  }

  public void setResetOffsetValueWhenStart(long resetOffsetValueWhenStart) {
    this.resetOffsetValueWhenStart = resetOffsetValueWhenStart;
    Utils.checkParameterRange(
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_START_RESET_OFFSET_VALUE,
        (int) resetOffsetValueWhenStart,
        (int) TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_START_RESET_OFFSET_AS_END,
        (int) TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_START_RESET_OFFSET_AS_START);
  }
}
