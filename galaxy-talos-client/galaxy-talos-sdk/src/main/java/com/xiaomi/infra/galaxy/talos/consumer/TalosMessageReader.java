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

public class TalosMessageReader extends MessageReader {
  private static final Logger LOG = LoggerFactory.getLogger(TalosMessageReader.class);

  public TalosMessageReader(TalosConsumerConfig talosConsumerConfig) {
    super(talosConsumerConfig);
  }

  @Override
  public void initStartOffset() throws Exception {
    startOffset.set(queryStartOffset());
    // guarantee lastCommitOffset and finishedOffset correct
    lastCommitOffset = finishedOffset = startOffset.get() - 1;
  }

  @Override
  public void commitCheckPoint() throws Exception {
    CheckPoint checkPoint = new CheckPoint(consumerGroup, topicAndPartition,
        finishedOffset, workerId);
    // check whether to check last commit offset
    if (consumerConfig.isCheckLastCommitOffset()) {
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
        lastCommitOffset + " for partition: " +
        topicAndPartition.getPartitionId());
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
      messageProcessor.process(messageList);
      finishedOffset = messageList.get(messageList.size() - 1).getMessageOffset();
      startOffset.set(finishedOffset + 1);
      if (shoudCommit()) {
        commitCheckPoint();
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

}