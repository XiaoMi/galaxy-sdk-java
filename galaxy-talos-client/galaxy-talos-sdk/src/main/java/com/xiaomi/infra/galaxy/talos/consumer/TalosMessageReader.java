/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: yongxing@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.consumer;

import java.util.List;

import libthrift091.TException;
import org.omg.CORBA.TRANSACTION_MODE;
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
    startOffset.set(queryStartOffset());
    // guarantee lastCommitOffset and finishedOffset correct
    lastCommitOffset = finishedOffset = startOffset.get() - 1;
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
        return;
      }

      /**
       * Note: We guarantee the committed offset must be the messages that
       * have been processed by user's MessageProcessor;
       */
      finishedOffset = messageList.get(messageList.size() - 1).getMessageOffset();
      messageProcessor.process(messageList, this);
      startOffset.set(finishedOffset + 1);
      if (shoudCommit()) {
        try {
          innerCheckpoint();
        } catch (TException e) {
          // when commitOffset failed, we just do nothing;
        }
      }
    } catch (Throwable e) {
      LOG.error("Error: " + e.toString() + " when getting messages from topic: " +
          topicAndPartition.getTopicTalosResourceName() + " partition: " +
          topicAndPartition.getPartitionId());

      processFetchException(e);
      lastFetchTime = System.currentTimeMillis();
    } // catch
  } // fetchData

  private long queryStartOffset() throws TException {
    QueryOffsetRequest queryOffsetRequest = new QueryOffsetRequest(
        consumerGroup, topicAndPartition);
    QueryOffsetResponse queryOffsetResponse = consumerClient.queryOffset(
        queryOffsetRequest);
    // startOffset = queryOffset + 1
    return queryOffsetResponse.getMsgOffset() + 1;
  }

  private void innerCheckpoint() throws TException {
    if (consumerConfig.isCheckpointMessageOffset()) {
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
    if (consumerConfig.isCheckpointMessageOffset()) {
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
      return false;
    }
  }

  private void commitOffset(long messageOffset) throws TException {
    CheckPoint checkPoint = new CheckPoint(consumerGroup, topicAndPartition,
        messageOffset, workerId);
    // check whether to check last commit offset
    if (consumerConfig.isCheckLastCommitOffset()) {
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
      LOG.info("Worker: " + workerId + " commit offset: " +
          lastCommitOffset + " for partition: " +
          topicAndPartition.getPartitionId() + " failed");
    }
  }
}