/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: yongxing@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.consumer;

import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  public MessageReader(TalosConsumerConfig consumerConfig) {
    this.consumerConfig = consumerConfig;
    lastCommitOffset = finishedOffset = -1;
    lastCommitTime = lastFetchTime = System.currentTimeMillis();
    startOffset = new AtomicLong(-1);
    commitThreshold = consumerConfig.getCommitOffsetThreshold();
    commitInterval = consumerConfig.getCommitOffsetInterval();
    fetchInterval = consumerConfig.getFetchMessageInterval();
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

  public AtomicLong getStartOffset() {
    return startOffset;
  }

  protected boolean shoudCommit() {
    return (System.currentTimeMillis() - lastCommitTime >= commitInterval) ||
        (finishedOffset - lastCommitOffset >= commitThreshold);
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

}
