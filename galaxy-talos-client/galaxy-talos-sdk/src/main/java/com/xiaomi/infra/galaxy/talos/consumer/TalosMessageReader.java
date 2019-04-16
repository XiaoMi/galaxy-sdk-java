/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: yongxing@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.consumer;

import java.util.List;

import libthrift091.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.infra.galaxy.talos.thrift.CheckPoint;
import com.xiaomi.infra.galaxy.talos.thrift.MessageAndOffset;
import com.xiaomi.infra.galaxy.talos.thrift.QueryOffsetRequest;
import com.xiaomi.infra.galaxy.talos.thrift.QueryOffsetResponse;
import com.xiaomi.infra.galaxy.talos.thrift.UpdateOffsetRequest;
import com.xiaomi.infra.galaxy.talos.thrift.UpdateOffsetResponse;

public class TalosMessageReader extends MessageReader implements MessageCheckpointer {
  private static final Logger LOG = LoggerFactory.getLogger(TalosMessageReader.class);

  public TalosMessageReader(TalosConsumerConfig talosConsumerConfig) {
    super(talosConsumerConfig);
  }

  @Override
  public void initStartOffset() throws Exception {
    // get last commit offset or init by outer checkPoint
    long readingStartOffset;
    if (outerCheckPoint != null && outerCheckPoint >= 0) {
      readingStartOffset = outerCheckPoint;
      // burn after reading the first time
      outerCheckPoint = null;
    } else {
      readingStartOffset = queryStartOffset();
    }

    // when consumer starting up, checking:
    // 1) whether not exist last commit offset, which means 'readingStartOffset==-1'
    // 2) whether reset offset
    // 3) note that: the priority of 'reset-config' is larger than 'outer-checkPoint'
    if (readingStartOffset == -1 || consumerConfig.isResetOffsetWhenStart()) {
      startOffset.set(consumerConfig.getResetOffsetValueWhenStart());
    } else {
      startOffset.set(readingStartOffset);
    }

    // guarantee lastCommitOffset and finishedOffset correct
    if (startOffset.longValue() > 0) {
      lastCommitOffset = finishedOffset = startOffset.get() - 1;
    }
    LOG.info("Init startOffset: " + startOffset + " lastCommitOffset: " +
        lastCommitOffset + " for partition: " + topicAndPartition);
    messageProcessor.init(topicAndPartition, startOffset.get());
  }

  @Override
  public void commitCheckPoint() throws Exception {
    innerCheckpoint();
    messageProcessor.shutdown(this);
  }

  @Override
  public void fetchData() {
    // control fetch qps
    if (System.currentTimeMillis() - lastFetchTime < fetchInterval) {
      try {
        Thread.sleep(lastFetchTime + fetchInterval - System.currentTimeMillis());
      } catch (InterruptedException e) {
        // do nothing
      }
    }

    // fetch data and process them
    try {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Reading message from offset: " + startOffset.get() +
            " of partition: " + topicAndPartition.getPartitionId());
      }
      List<MessageAndOffset> messageList = simpleConsumer.fetchMessage(
          startOffset.get());
      lastFetchTime = System.currentTimeMillis();
      // return when no message got
      if (messageList == null || messageList.size() == 0) {
        checkAndCommit(false);
        return;
      }

      /**
       * Note: We guarantee the committed offset must be the messages that
       * have been processed by user's MessageProcessor;
       */
      finishedOffset = messageList.get(messageList.size() - 1).getMessageOffset();
      messageProcessor.process(messageList, this);
      startOffset.set(finishedOffset + 1);
      checkAndCommit(true);
    } catch (Throwable e) {
      LOG.error("Error when getting messages from topic: " +
          topicAndPartition.getTopicTalosResourceName() + " partition: " +
          topicAndPartition.getPartitionId(), e);

      processFetchException(e);
      lastFetchTime = System.currentTimeMillis();
    } // catch
  } // fetchData

  private long queryStartOffset() throws TException {
    QueryOffsetRequest queryOffsetRequest = new QueryOffsetRequest(
        consumerGroup, topicAndPartition);
    QueryOffsetResponse queryOffsetResponse = consumerClient.queryOffset(
        queryOffsetRequest);

    long committedOffset = queryOffsetResponse.getMsgOffset();
    // 'committedOffset == -1' means not exist last committed offset
    // startOffset = committedOffset + 1
    return ((committedOffset == -1) ? -1 : committedOffset + 1);
  }

  private void innerCheckpoint() throws TException {
    if (consumerConfig.isCheckpointAutoCommit()) {
      commitOffset(finishedOffset);
    }
  }

  @Override
  public boolean checkpoint() {
    return checkpoint(finishedOffset);
  }

  @Override
  public boolean checkpoint(long messageOffset) {
    LOG.info("start checkpoint: " + messageOffset);
    if (consumerConfig.isCheckpointAutoCommit()) {
      LOG.info("You can not checkpoint through MessageCheckpointer when you set " +
          "\"galaxy.talos.consumer.checkpoint.message.offset\" as \"true\"");
      return false;
    }

    if (messageOffset <= lastCommitOffset || messageOffset > finishedOffset) {
      LOG.info("checkpoint messageOffset: " + messageOffset + " in wrong " +
          "range, lastCheckpoint messageOffset: " + lastCommitOffset + ", last " +
          "deliver messageOffset: " + finishedOffset);
      return false;
    }

    try {
      commitOffset(messageOffset);
      return true;
    } catch (TException e) {
      LOG.error("Error when getting messages from topic: " +
          topicAndPartition.getTopicTalosResourceName() + " partition: " +
          topicAndPartition.getPartitionId(), e);
      return false;
    }
  }

  private void commitOffset(long messageOffset) throws TException {
    CheckPoint checkPoint = new CheckPoint(consumerGroup, topicAndPartition,
        messageOffset, workerId);
    // check whether to check last commit offset, firstCommit do not check
    if ((lastCommitOffset != -1) && consumerConfig.isCheckLastCommitOffset()) {
      checkPoint.setLastCommitOffset(lastCommitOffset);
    }

    UpdateOffsetRequest updateOffsetRequest = new UpdateOffsetRequest(checkPoint);
    UpdateOffsetResponse updateOffsetResponse = consumerClient.updateOffset(
        updateOffsetRequest);
    // update startOffset as next message
    if (updateOffsetResponse.isSuccess()) {
      lastCommitOffset = messageOffset;
      lastCommitTime = System.currentTimeMillis();
      LOG.info("Worker: " + workerId + " commit offset: " +
          lastCommitOffset + " for partition: " +
          topicAndPartition.getPartitionId());
    } else {
      LOG.error("Worker: " + workerId + " commit offset: " +
          lastCommitOffset + " for partition: " +
          topicAndPartition.getPartitionId() + " failed");
    }
  }

  private void checkAndCommit(boolean isContinuous){
    if (shouldCommit(isContinuous)) {
      try {
        innerCheckpoint();
      } catch (TException e) {
        // when commitOffset failed, we just do nothing;
        LOG.error("commit offset error, we skip to it", e);
      }
    }
  }
}