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
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Preconditions;
import libthrift091.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.infra.galaxy.talos.client.Utils;
import com.xiaomi.infra.galaxy.talos.thrift.CheckPoint;
import com.xiaomi.infra.galaxy.talos.thrift.ConsumeUnit;
import com.xiaomi.infra.galaxy.talos.thrift.ConsumerService;
import com.xiaomi.infra.galaxy.talos.thrift.LockPartitionRequest;
import com.xiaomi.infra.galaxy.talos.thrift.LockPartitionResponse;
import com.xiaomi.infra.galaxy.talos.thrift.MessageAndOffset;
import com.xiaomi.infra.galaxy.talos.thrift.MessageService;
import com.xiaomi.infra.galaxy.talos.thrift.QueryOffsetRequest;
import com.xiaomi.infra.galaxy.talos.thrift.QueryOffsetResponse;
import com.xiaomi.infra.galaxy.talos.thrift.TopicAndPartition;
import com.xiaomi.infra.galaxy.talos.thrift.TopicTalosResourceName;
import com.xiaomi.infra.galaxy.talos.thrift.UnlockPartitionRequest;
import com.xiaomi.infra.galaxy.talos.thrift.UpdateOffsetRequest;
import com.xiaomi.infra.galaxy.talos.thrift.UpdateOffsetResponse;

/**
 * PartitionFetcher
 *
 * Per partition per PartitionFetcher
 *
 * PartitionFetcher as the message process task for one partition, which has four state:
 * INIT, LOCKED, UNLOCKING, UNLOCKED
 * Every PartitionFetcher has one runnable messageReader to fetch messages continuously.
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
  }

  private class MessageReader implements Runnable {
    private final int commitThreshold = talosConsumerConfig.getCommitOffsetThreshold();
    private final int commitInterval = talosConsumerConfig.getCommitOffsetInterval();
    private final int fetchInterval = talosConsumerConfig.getFetchMessageInterval();

    private AtomicLong startOffset;
    private long finishedOffset;
    private MessageProcessor messageProcessor;
    private long lastCommitTime;
    private long lastCommitOffset;
    private long lastFetchTime;

    private MessageReader(MessageProcessor messageProcessor) {
      this.messageProcessor = messageProcessor;
      lastCommitTime = lastFetchTime = System.currentTimeMillis();
      lastCommitOffset = finishedOffset = -1;
      startOffset = new AtomicLong(-1);
      LOG.info("initialize message reader for partition: " + partitionId);
    }

    private boolean shoudCommit() {
      return (System.currentTimeMillis() - lastCommitTime >= commitInterval) ||
          (finishedOffset - lastCommitOffset >= commitThreshold);
    }

    // commit the last processed offset and update the startOffset
    private void commitOffset() throws TException {
      CheckPoint checkPoint = new CheckPoint(consumerGroup, topicAndPartition,
          finishedOffset, workerId);
      // check whether to check last commit offset
      if (talosConsumerConfig.isCheckLastCommitOffset()) {
        checkPoint.setLastCommitOffset(lastCommitOffset);
      }

      UpdateOffsetRequest updateOffsetRequest = new UpdateOffsetRequest(checkPoint);
      UpdateOffsetResponse updateOffsetResponse = consumerClient.updateOffset(
          updateOffsetRequest);
      // update startOffset as next message
      if (updateOffsetResponse.isSuccess()) {
        lastCommitOffset = finishedOffset;
        lastCommitTime = System.currentTimeMillis();
      }

      LOG.info("Worker: " + workerId + " commit offset: " +
          lastCommitOffset + " for partition: " + partitionId);
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
        startOffset.set(getStartOffset());
        // guarantee lastCommitOffset and finishedOffset correct
        lastCommitOffset = finishedOffset = startOffset.get() - 1;
      } catch (Throwable e) {
        LOG.error("Worker: " + workerId + " query partition offset error: " +
            e.toString() + " skip this partition");
        clean();
        return;
      }

      // reading data
      LOG.info("The workerId: " + workerId + " is serving partition: " +
          partitionId + " from offset: " + startOffset.get());
      while (getCurState() == TASK_STATE.LOCKED) {
        // control fetch qps
        if (System.currentTimeMillis() - lastFetchTime < fetchInterval) {
          try {
            Thread.sleep(lastFetchTime + fetchInterval - System.currentTimeMillis());
          } catch (InterruptedException e) {
            // do nothing
          }
        }

        try {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Reading message from offset: " + startOffset.get() +
                " of partition: " + partitionId);
          }
          List<MessageAndOffset> messageList = simpleConsumer.fetchMessage(
              startOffset.get());
          lastFetchTime = System.currentTimeMillis();
          if (messageList == null || messageList.size() == 0) {
            continue;
          }

          /**
           * Note: We guarantee the committed offset must be the messages that
           * have been processed by user's MessageProcessor;
           */
          messageProcessor.process(messageList);
          finishedOffset = messageList.get(messageList.size() - 1).getMessageOffset();
          startOffset.set(finishedOffset + 1);
          if (shoudCommit()) {
            commitOffset();
          }
        } catch (Throwable e) {
          LOG.error("Error: " + e.toString() + " when getting messages from topic: " +
              topicTalosResourceName + " partition: " + partitionId);

          // delay when partitionNotServing
          if (Utils.isPartitionNotServing(e)) {
            LOG.warn("Partition: " + partitionId +
                " is not serving state, sleep a while for waiting it work.");
            try {
              Thread.sleep(talosConsumerConfig.getWaitPartitionWorkingTime());
            } catch (InterruptedException e1) {
              e1.printStackTrace();
            }
          } // if

          lastFetchTime = System.currentTimeMillis();
        } // catch
      }

      // wait task quit gracefully: stop reading, commit offset, clean and shutdown
      if (finishedOffset > lastCommitOffset) {
        try {
          commitOffset();
        } catch (TException e) {
          LOG.error("Error: " + e.toString() + " when commit offset for topic: " +
              topicTalosResourceName + " partition: " + partitionId);
        }
      }

      clean();
      LOG.info("The MessageProcessTask for topic: " + topicTalosResourceName +
          " partition: " + partitionId + " is finished");
    }
  } // MessageReader

  private static final Logger LOG = LoggerFactory.getLogger(PartitionFetcher.class);
  private String consumerGroup;
  private TopicTalosResourceName topicTalosResourceName;
  private int partitionId;
  private TalosConsumerConfig talosConsumerConfig;
  private String workerId;
  private ConsumerService.Iface consumerClient;
  private MessageProcessor messageProcessor;
  private TASK_STATE curState;
  private ExecutorService singleExecutor;
  private Future fetcherFuture;

  private TopicAndPartition topicAndPartition;
  private SimpleConsumer simpleConsumer;

  public PartitionFetcher(String consumerGroup, String topicName,
      TopicTalosResourceName topicTalosResourceName, int partitionId,
      TalosConsumerConfig talosConsumerConfig, String workerId,
      ConsumerService.Iface consumerClient, MessageService.Iface messageClient,
      MessageProcessor messageProcessor) {
    this.consumerGroup = consumerGroup;
    this.topicTalosResourceName = topicTalosResourceName;
    this.partitionId = partitionId;
    this.talosConsumerConfig = talosConsumerConfig;
    this.workerId = workerId;
    this.consumerClient = consumerClient;
    this.messageProcessor = messageProcessor;
    curState = TASK_STATE.INIT;
    singleExecutor = Executors.newSingleThreadExecutor();
    fetcherFuture = null;

    topicAndPartition = new TopicAndPartition(topicName,
        topicTalosResourceName, partitionId);
    simpleConsumer = new SimpleConsumer(talosConsumerConfig, topicAndPartition,
        messageClient);
    LOG.info("The PartitionFetcher for topic: " + topicTalosResourceName +
        " partition: " + partitionId + " init.");
  }

  // for test
  public PartitionFetcher(String consumerGroup, String topicName,
      TopicTalosResourceName topicTalosResourceName, int partitionId,
      TalosConsumerConfig talosConsumerConfig, String workerId,
      ConsumerService.Iface consumerClient, SimpleConsumer simpleConsumer,
      MessageProcessor messageProcessor) {
    this.consumerGroup = consumerGroup;
    this.topicTalosResourceName = topicTalosResourceName;
    this.partitionId = partitionId;
    this.talosConsumerConfig = talosConsumerConfig;
    this.workerId = workerId;
    this.consumerClient = consumerClient;
    this.messageProcessor = messageProcessor;
    curState = TASK_STATE.INIT;
    singleExecutor = Executors.newSingleThreadExecutor();
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
      MessageReader messageReader = new MessageReader(messageProcessor);
      fetcherFuture = singleExecutor.submit(messageReader);
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

  public void shutDown() {
    if (fetcherFuture != null) {
      LOG.info("worker: " + workerId + " try to shutdown partition: " +
          partitionId);
      // set UNLOCKING to stop read and wait fetcher gracefully quit
      updateState(TASK_STATE.UNLOCKING);
      // 'false' means wait task done
      fetcherFuture.cancel(false);
    }
    singleExecutor.shutdownNow();
  }

  private synchronized TASK_STATE getCurState() {
    return curState;
  }

  private synchronized boolean updateState(TASK_STATE targetState) {
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
      LOG.warn("Worker: " + workerId + " release partition error: " + e.toString());
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
      LOG.error("Worker: " + workerId + " steal partition error: " + e.toString());
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

  private long getStartOffset() throws TException {
    QueryOffsetRequest queryOffsetRequest = new QueryOffsetRequest(
        consumerGroup, topicAndPartition);
    QueryOffsetResponse queryOffsetResponse = consumerClient.queryOffset(
        queryOffsetRequest);
    // startOffset = queryOffset + 1
    return queryOffsetResponse.getMsgOffset() + 1;
  }

  // unlock partitionLock, then revoke this task and set it to 'UNLOCKED'
  private void clean() {
    releasePartition();
    updateState(TASK_STATE.UNLOCKED);
  }
}
