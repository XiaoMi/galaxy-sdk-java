/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: yongxing@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.consumer;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.infra.galaxy.talos.client.Constants;
import com.xiaomi.infra.galaxy.talos.client.Utils;
import com.xiaomi.infra.galaxy.talos.thrift.ConsumerService;
import com.xiaomi.infra.galaxy.talos.thrift.MessageOffset;
import com.xiaomi.infra.galaxy.talos.thrift.TopicAndPartition;

/**
 * The base class using by PartitionFetcher
 */

public abstract class MessageReader {
  private static final Logger LOG = LoggerFactory.getLogger(MessageReader.class);
  protected int commitThreshold;
  protected int commitInterval;
  protected int fetchInterval;

  protected long lastCommitTime;
  protected long lastFetchTime;

  protected AtomicLong startOffset;
  protected long finishedOffset;
  protected long lastCommitOffset;
  protected MessageProcessor messageProcessor;

  protected String workerId;
  protected String consumerGroup;
  protected TopicAndPartition topicAndPartition;
  protected TalosConsumerConfig consumerConfig;
  protected SimpleConsumer simpleConsumer;
  protected ConsumerService.Iface consumerClient;

  // outer-checkPoint can be used only one time, burn after reading to prevent
  // partitionFetcher re-lock() and re-use outer-checkPoint again by consumer re-balance
  protected Long outerCheckPoint;

  protected ConsumerMetrics consumerMetrics;

  public MessageReader(TalosConsumerConfig consumerConfig) {
    this.consumerConfig = consumerConfig;
    lastCommitOffset = finishedOffset = -1;
    lastCommitTime = lastFetchTime = System.currentTimeMillis();
    startOffset = new AtomicLong(-1);
    commitThreshold = consumerConfig.getCommitOffsetThreshold();
    commitInterval = consumerConfig.getCommitOffsetInterval();
    fetchInterval = consumerConfig.getFetchMessageInterval();
    outerCheckPoint = null;
  }

  public MessageReader setWorkerId(String workerId) {
    this.workerId = workerId;
    return this;
  }

  public MessageReader setConsumerGroup(String consumerGroup) {
    Utils.checkNameValidity(consumerGroup);
    this.consumerGroup = consumerGroup;
    return this;
  }

  public MessageReader initConsumerMetrics() {
    this.consumerMetrics = new ConsumerMetrics();
    return this;
  }

  public MessageReader initGreedyMetrics(int partitionId) {
    this.consumerMetrics = new ConsumerMetrics(partitionId);
    return this;
  }

  public MessageReader setTopicAndPartition(TopicAndPartition topicAndPartition) {
    this.topicAndPartition = topicAndPartition;
    return this;
  }

  public MessageReader setSimpleConsumer(SimpleConsumer simpleConsumer) {
    this.simpleConsumer = simpleConsumer;
    return this;
  }

  public MessageReader setMessageProcessor(MessageProcessor messageProcessor) {
    this.messageProcessor = messageProcessor;
    return this;
  }

  public MessageReader setConsumerClient(ConsumerService.Iface consumerClient) {
    this.consumerClient = consumerClient;
    return this;
  }

  public void setOuterCheckPoint(Long outerCheckPoint) {
    this.outerCheckPoint = outerCheckPoint;
  }

  public AtomicLong getStartOffset() {
    return startOffset;
  }

  public long getCurCheckPoint() {
    // init state or before the first committing
    if (lastCommitOffset <= startOffset.get()) {
      return startOffset.get();
    }

    // From lastCommitOffset + 1 when reading next time
    return lastCommitOffset + 1;
  }

  public ConsumerMetrics getConsumerMetrics() {
    return consumerMetrics;
  }

  protected boolean shouldCommit(boolean isContinuous) {
    if (isContinuous) {
      return (System.currentTimeMillis() - lastCommitTime >= commitInterval) ||
          (finishedOffset - lastCommitOffset >= commitThreshold);
    } else {
      return (System.currentTimeMillis() - lastCommitTime >= commitInterval) &&
          (finishedOffset > lastCommitOffset);
    }
  }

  protected void cleanReader() {
    // wait task quit gracefully: stop reading, commit offset, clean and shutdown
    if (finishedOffset > lastCommitOffset) {
      try {
        commitCheckPoint();
      } catch (Exception e) {
        LOG.error("Error when commit offset for topic: " +
            topicAndPartition.getTopicTalosResourceName() +
            " partition: " + topicAndPartition.getPartitionId(), e);
      }
    }
  }

  protected void processFetchException(Throwable e) {
    // delay when partitionNotServing
    if (Utils.isPartitionNotServing(e)) {
      LOG.warn("Partition: " + topicAndPartition.getPartitionId() +
          " is not serving state, sleep a while for waiting it work.", e);
      try {
        Thread.sleep(consumerConfig.getWaitPartitionWorkingTime());
      } catch (InterruptedException e1) {
        e1.printStackTrace();
      }
    } // if

    // process message offset out of range, reset start offset
    if (Utils.isOffsetOutOfRange(e)) {
      if (consumerConfig.isResetLatestOffsetWhenOutOfRange()) {
        LOG.warn("Got PartitionOutOfRange error, " +
            " offset by current latest offset", e);
        startOffset.set(MessageOffset.LATEST_OFFSET.getValue());
        lastCommitOffset = finishedOffset = - 1;
        lastCommitTime = System.currentTimeMillis();
      } else {
        LOG.warn("Got PartitionOutOfRange error," +
            " reset offset by current start offset", e);
        startOffset.set(MessageOffset.START_OFFSET.getValue());
        lastCommitOffset = finishedOffset = - 1;
        lastCommitTime = System.currentTimeMillis();
      }
    } // if

    LOG.warn("process unexcepted fetchException:", e);
  }

  /**
   * query start offset to read, if failed, throw the exception
   */
  public abstract void initStartOffset() throws Exception;

  /**
   * commit the last processed offset and update the startOffset
   * throw the Exception when appear error
   */
  public abstract void commitCheckPoint() throws Exception;

  /**
   * you should implement this method as follow process:
   * 1. control fetch qps by fetchInterval
   * 2. fetchMessage with try/catch structure and process exception
   * 2.1 catch chunk process PARTITION_NOT_SERVING by sleep a while
   * 2.2 catch chunk process MESSAGE_OFFSET_OUT_OF_RANGE by fixOffsetOutofRange()
   * 2.3 reset lastFetchTime
   * 3. process fetched message by MessageProcessor and update finishedOffset/startOffset
   * 4. check whether should commit offset
   */
  public abstract void fetchData();

  public class ConsumerMetrics {
    private String falconEndpoint;
    private long fetchDuration;
    private long maxFetchDuration;
    private long minFetchDuration;
    private long processDuration;
    private long maxProcessDuration;
    private long minProcessDuration;
    private int fetchTimes;
    private int fetchFailedTimes;
    private Map<String, Number> consumerMetricsMap;

    private ConsumerMetrics() {
      initMetrics();
      this.falconEndpoint = consumerConfig.getConsumerMetricFalconEndpoint() +
          consumerGroup;
      this.consumerMetricsMap = new LinkedHashMap<String, Number>();
    }

    private ConsumerMetrics(int partitionId) {
      initMetrics();
      this.falconEndpoint = consumerConfig.getGreedyMetricFalconEndpoint() +
          workerId;
      this.consumerMetricsMap = new LinkedHashMap<String, Number>();
    }

    private void initMetrics() {
      this.fetchDuration = 0;
      this.maxFetchDuration = 0;
      this.minFetchDuration = 0;
      this.processDuration = 0;
      this.maxProcessDuration = 0;
      this.minProcessDuration = 0;
      this.fetchTimes = 0;
      this.fetchFailedTimes = 0;
    }

    public void markFetchDuration(long fetchDuration) {
      if (fetchDuration > maxFetchDuration) {
        this.maxFetchDuration = fetchDuration;
      }

      if (minFetchDuration == 0 || fetchDuration < minFetchDuration) {
        this.minFetchDuration = fetchDuration;
      }

      this.fetchDuration = fetchDuration;
      this.fetchTimes += 1;
    }

    public void markFetchOrProcessFailedTimes() {
      this.fetchFailedTimes += 1;
    }

    public void markProcessDuration(long processDuration) {
      if (processDuration > maxProcessDuration) {
        this.maxProcessDuration = processDuration;
      }

      if (minProcessDuration == 0 || processDuration < minProcessDuration) {
        this.minProcessDuration = processDuration;
      }

      this.processDuration = processDuration;
    }

    public JsonArray toJsonData() {
      updateMetricsMap();
      JsonArray jsonArray = new JsonArray();
      for (Map.Entry<String, Number> entry : consumerMetricsMap.entrySet()) {
        JsonObject jsonObject = getBasicData();
        jsonObject.addProperty("metric", entry.getKey());
        jsonObject.addProperty("value", entry.getValue().doubleValue());
        jsonArray.add(jsonObject);
      }
      initMetrics();
      return jsonArray;
    }

    private void updateMetricsMap() {
      consumerMetricsMap.put(Constants.FETCH_MESSAGE_TIME, fetchDuration);
      consumerMetricsMap.put(Constants.MAX_FETCH_MESSAGE_TIME, maxFetchDuration);
      consumerMetricsMap.put(Constants.MIN_FETCH_MESSAGE_TIME, minFetchDuration);
      consumerMetricsMap.put(Constants.PROCESS_MESSAGE_TIME, processDuration);
      consumerMetricsMap.put(Constants.MAX_PROCESS_MESSAGE_TIME, maxProcessDuration);
      consumerMetricsMap.put(Constants.MIN_PROCESS_MESSAGE_TIME, minProcessDuration);
      consumerMetricsMap.put(Constants.FETCH_MESSAGE_TIMES, fetchTimes / 60.0);
      consumerMetricsMap.put(Constants.FETCH_MESSAGE_FAILED_TIMES, fetchFailedTimes / 60.0);
    }

    private JsonObject getBasicData() {
      String tag = "clusterName=" + consumerConfig.getClusterName();
      tag += ",topicName=" + topicAndPartition.getTopicName();
      tag += ",partitionId=" + topicAndPartition.getPartitionId();
      tag += ",ip=" + consumerConfig.getClientIp();
      tag += ",type=" + consumerConfig.getAlertType();

      JsonObject basicData = new JsonObject();
      basicData.addProperty("endpoint", falconEndpoint);
      basicData.addProperty("timestamp", System.currentTimeMillis() / 1000);
      basicData.addProperty("step", consumerConfig.getMetricFalconStep() / 1000);
      basicData.addProperty("counterType", "GAUGE");
      basicData.addProperty("tags", tag);
      return basicData;
    }
  }
}
