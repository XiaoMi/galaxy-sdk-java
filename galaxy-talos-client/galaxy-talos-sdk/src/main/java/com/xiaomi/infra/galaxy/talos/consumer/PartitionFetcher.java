/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: yongxing@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.consumer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.google.gson.JsonArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.infra.galaxy.talos.client.NamedThreadFactory;
import com.xiaomi.infra.galaxy.talos.thrift.ConsumeUnit;
import com.xiaomi.infra.galaxy.talos.thrift.ConsumerService;
import com.xiaomi.infra.galaxy.talos.thrift.LockPartitionRequest;
import com.xiaomi.infra.galaxy.talos.thrift.LockPartitionResponse;
import com.xiaomi.infra.galaxy.talos.thrift.MessageOffset;
import com.xiaomi.infra.galaxy.talos.thrift.MessageService;
import com.xiaomi.infra.galaxy.talos.thrift.TopicAndPartition;
import com.xiaomi.infra.galaxy.talos.thrift.TopicTalosResourceName;
import com.xiaomi.infra.galaxy.talos.thrift.UnlockPartitionRequest;

/**
 * PartitionFetcher
 *
 * Per partition per PartitionFetcher
 *
 * PartitionFetcher as the message process task for one partition, which has four state:
 * INIT, LOCKED, UNLOCKING, UNLOCKED
 * Every PartitionFetcher has one runnable FetcherStateMachine to fetch messages continuously.
 *
 * when standing be LOCKED, it continuously reading messages by SimpleConsumer.fetchMessage;
 * when standing be UNLOCKING, it stop to read, commit offset and release the partition lock;
 * when standing be UNLOCKED, it do not serve any partition and wait to be invoking;
 */

public class PartitionFetcher {

  /**
   * State of PartitionFetcher
   *
   * The state evolution as follows:
   * INIT -> LOCKED;
   * LOCKED -> UNLOCKING;
   * LOCKED -> UNLOCKED;
   * UNLOCKING -> UNLOCKED;
   * UNLOCKED -> LOCKED;
   */
  private enum TASK_STATE {
    INIT,
    LOCKED,
    UNLOCKING,
    UNLOCKED,
    SHUTDOWNED,
  }

  private class FetcherStateMachine implements Runnable {
    private MessageReader messageReader;

    private FetcherStateMachine(MessageReader messageReader) {
      this.messageReader = messageReader;
      LOG.info("initialize FetcherStateMachine for partition: " + partitionId);
    }

    @Override
    public void run() {
      // try to lock partition from HBase, if failed, set to UNLOCKED and return;
      if (!stealPartition()) {
        updateState(TASK_STATE.UNLOCKED);
        return;
      }

      // query start offset to read, if failed, clean and return;
      try {
        messageReader.initStartOffset();
      } catch (Throwable e) {
        LOG.error("Worker: " + workerId + " query partition offset error: " +
            "we will skip this partition", e);
        clean();
        return;
      }

      // reading data
      LOG.info("The workerId: " + workerId + " is serving partition: " +
          partitionId + " from offset: " + messageReader.getStartOffset().get());
      while (getCurState() == TASK_STATE.LOCKED) {
        messageReader.fetchData();
      }

      // wait task quit gracefully: stop reading, commit offset, clean and shutdown
      messageReader.cleanReader();
      clean();
      LOG.info("The MessageProcessTask for topic: " + topicTalosResourceName +
          " partition: " + partitionId + " is finished");
    }
  } // FetcherStateMachine

  private static final Logger LOG = LoggerFactory.getLogger(PartitionFetcher.class);
  private String consumerGroup;
  private TopicTalosResourceName topicTalosResourceName;
  private int partitionId;
  private String workerId;
  private ConsumerService.Iface consumerClient;
  private TASK_STATE curState;
  private ExecutorService singleExecutor;
  private Future fetcherFuture;

  private TopicAndPartition topicAndPartition;
  private SimpleConsumer simpleConsumer;
  private MessageReader messageReader;

  public PartitionFetcher(String consumerGroup, String topicName,
      TopicTalosResourceName topicTalosResourceName, int partitionId,
      TalosConsumerConfig talosConsumerConfig, String workerId,
      ConsumerService.Iface consumerClient, MessageService.Iface messageClient,
      MessageProcessor messageProcessor, MessageReader messageReader,
      Long outerCheckPoint) {
    this.consumerGroup = consumerGroup;
    this.topicTalosResourceName = topicTalosResourceName;
    this.partitionId = partitionId;
    this.workerId = workerId;
    this.consumerClient = consumerClient;
    curState = TASK_STATE.INIT;
    singleExecutor = Executors.newSingleThreadExecutor(new NamedThreadFactory(
        "talos-consumer-" + consumerGroup + "-" + topicName + ":" + partitionId));
    fetcherFuture = null;

    topicAndPartition = new TopicAndPartition(topicName,
        topicTalosResourceName, partitionId);
    simpleConsumer = new SimpleConsumer(talosConsumerConfig, topicAndPartition,
        messageClient);

    // set MessageReader
    messageReader.setWorkerId(workerId)
        .setConsumerGroup(consumerGroup)
        .setTopicAndPartition(topicAndPartition)
        .initConsumerMetrics()
        .setSimpleConsumer(simpleConsumer)
        .setMessageProcessor(messageProcessor)
        .setConsumerClient(consumerClient)
        .setOuterCheckPoint(outerCheckPoint);
    this.messageReader = messageReader;

    LOG.info("The PartitionFetcher for topic: " + topicTalosResourceName +
        " partition: " + partitionId + " init.");
  }

  // for test
  public PartitionFetcher(String consumerGroup, String topicName,
      TopicTalosResourceName topicTalosResourceName, int partitionId,
      String workerId, ConsumerService.Iface consumerClient,
      SimpleConsumer simpleConsumer,MessageReader messageReader) {
    this.consumerGroup = consumerGroup;
    this.topicTalosResourceName = topicTalosResourceName;
    this.partitionId = partitionId;
    this.workerId = workerId;
    this.consumerClient = consumerClient;
    this.messageReader = messageReader;
    curState = TASK_STATE.INIT;
    singleExecutor = Executors.newSingleThreadExecutor(new NamedThreadFactory(
        "talos-consumer-" + consumerGroup + "-" + topicName + ":" + partitionId));
    fetcherFuture = null;

    topicAndPartition = new TopicAndPartition(topicName,
        topicTalosResourceName, partitionId);
    this.simpleConsumer = simpleConsumer;
  }

  // used to know whether is serving and reading data
  public synchronized boolean isServing() {
    return (curState == TASK_STATE.LOCKED);
  }

  // used to know whether need to renew
  public synchronized boolean isHoldingLock() {
    return (curState == TASK_STATE.LOCKED || curState == TASK_STATE.UNLOCKING);
  }

  /**
   * we want to guarantee the operation order for partitionFetcher,
   * such as process the following operation call:
   * 1) lock -> lock: the second 'lock' will be useless
   * 2) unlock -> unlock: the second 'unlock' will be useless
   * 3) lock -> unlock: every step within 'lock' can gracefully exit by unlock
   * 4) unlock -> lock: the 'lock' operation is useless before 'unlock' process done
   */
  // used for invoke this partition fetcher
  public void lock() {
    if (updateState(TASK_STATE.LOCKED)) {
      FetcherStateMachine fetcherStateMachine = new FetcherStateMachine(
          messageReader);
      fetcherFuture = singleExecutor.submit(fetcherStateMachine);
      LOG.info("Worker: " + workerId + " invoke partition: " +
          partitionId + " to 'LOCKED', try to serve it.");
    }
  }

  // used for revoke this partition fetcher async
  public void unlock() {
    if (updateState(TASK_STATE.UNLOCKING)) {
      LOG.info("Worker: " + workerId + " has set partition: " +
          partitionId + " to 'UNLOCKING', it is revoking gracefully.");
    }
  }

  public synchronized long getCurCheckPoint() {
    if (!isHoldingLock()) {
      return MessageOffset.START_OFFSET.getValue();
    }
    return messageReader.getCurCheckPoint();
  }

  public void shutDown() {
    // set UNLOCKING to stop read and wait fetcher gracefully quit
    updateState(TASK_STATE.UNLOCKING);

    if (fetcherFuture != null) {
      LOG.info("worker: " + workerId + " try to shutdown partition: " +
          partitionId);
      // 'false' means not stop the running task;
      fetcherFuture.cancel(false);
    }

    singleExecutor.shutdown();
    while (true) {
      try {
        if (singleExecutor.awaitTermination(100, TimeUnit.MILLISECONDS)) {
          break;
        }
      } catch (InterruptedException e) {

      }
    }

    updateState(TASK_STATE.SHUTDOWNED);
  }

  private synchronized TASK_STATE getCurState() {
    return curState;
  }

  private synchronized boolean updateState(TASK_STATE targetState) {
    LOG.info("PartitionFetcher for Partition: " + partitionId + " update " +
        "status from: " + curState + " to: " + targetState);
    switch (targetState) {
      case INIT:
        LOG.error("targetState can never be INIT, " +
            "updateState error for: " + partitionId);
        break;
      case LOCKED:
        if (curState == TASK_STATE.INIT || curState == TASK_STATE.UNLOCKED) {
          curState = TASK_STATE.LOCKED;
          return true;
        }
        LOG.error("targetState is LOCKED, but curState is: " + curState +
            " for partition: " + partitionId);
        break;
      case UNLOCKING:
        if (curState == TASK_STATE.LOCKED) {
          curState = TASK_STATE.UNLOCKING;
          return true;
        }
        LOG.error("targetState is UNLOCKING, but curState is: " + curState +
            " for partition: " + partitionId);
        break;
      case UNLOCKED:
        if (curState == TASK_STATE.UNLOCKING || curState == TASK_STATE.LOCKED) {
          curState = TASK_STATE.UNLOCKED;
          return true;
        }
        LOG.error("targetState is UNLOCKED, but curState is: " + curState +
            " for partition: " + partitionId);
        break;
      case SHUTDOWNED:
        curState = TASK_STATE.SHUTDOWNED;
      default:
    }
    return false;
  }

  /**
   * conditions for releasePartition:
   * 1) LOCKED, stealPartition success but get startOffset failed
   * 2) UNLOCKING, stop to serve this partition
   */
  private void releasePartition() {
    // release lock, if unlock failed, we just wait ttl work.
    List<Integer> toReleaseList = new ArrayList<Integer>();
    toReleaseList.add(partitionId);
    ConsumeUnit consumeUnit = new ConsumeUnit(consumerGroup,
        topicTalosResourceName, toReleaseList, workerId);
    UnlockPartitionRequest unlockRequest = new UnlockPartitionRequest(consumeUnit);
    try {
      consumerClient.unlockPartition(unlockRequest);
    } catch (Throwable e) {
      LOG.warn("Worker: " + workerId + " release partition error: ", e);
      return;
    }
    LOG.info("Worker: " + workerId + " success to release partition: " + partitionId);
  }

  private boolean stealPartition() {
    TASK_STATE state = getCurState();
    if (state != TASK_STATE.LOCKED) {
      LOG.error("Worker: " + workerId + " try to stealPartitionLock: " +
          partitionId + " but got state: " + state);
      return false;
    }

    // steal lock, if lock failed, we skip it and wait next re-balance
    List<Integer> toStealList = new ArrayList<Integer>();
    toStealList.add(partitionId);
    ConsumeUnit consumeUnit = new ConsumeUnit(consumerGroup,
        topicTalosResourceName, toStealList, workerId);
    LockPartitionRequest lockRequest = new LockPartitionRequest(consumeUnit);

    LockPartitionResponse lockResponse = null;
    try {
      lockResponse = consumerClient.lockPartition(lockRequest);
    } catch (Throwable e) {
      LOG.error("Worker: " + workerId + " steal partition error: ", e);
      return false;
    }

    // get the successfully locked partition
    List<Integer> successPartitionList = lockResponse.getSuccessPartitions();
    if (successPartitionList.size() > 0) {
      Preconditions.checkArgument(successPartitionList.get(0) == partitionId);
      LOG.info("Worker: " + workerId + " success to lock partitions: " +
          partitionId);
      return true;
    }
    LOG.error("Worker: " + workerId + " failed to lock partitions: " + partitionId);
    return false;
  }

  // unlock partitionLock, then revoke this task and set it to 'UNLOCKED'
  private void clean() {
    releasePartition();
    updateState(TASK_STATE.UNLOCKED);
  }

  public JsonArray getFalconData() {
    return messageReader.getConsumerMetrics().toJsonData();
  }
}