/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: yongxing@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.producer;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.infra.galaxy.talos.thrift.Message;

public class PartitionMessageQueue {
  private static final Logger LOG = LoggerFactory.getLogger(PartitionMessageQueue.class);
  private LinkedList<UserMessage> userMessageList;
  private int curMessageBytes;
  private int partitionId;
  private TalosProducer producer;

  private int maxBufferedTime;
  private int maxPutMsgNumber;
  private int maxPutMsgBytes;

  public PartitionMessageQueue(TalosProducerConfig producerConfig,
      int partitionId, TalosProducer producerPtr) {
    userMessageList = new LinkedList<UserMessage>();
    curMessageBytes = 0;
    this.partitionId = partitionId;
    producer = producerPtr;

    maxBufferedTime = producerConfig.getMaxBufferedMsgTime();
    maxPutMsgNumber = producerConfig.getMaxPutMsgNumber();
    maxPutMsgBytes = producerConfig.getMaxPutMsgBytes();
  }

  public synchronized void addMessage(List<UserMessage> messageList) {
    int incrementBytes = 0;
    for (UserMessage userMessage : messageList) {
      userMessageList.addFirst(userMessage);
      incrementBytes += userMessage.getMessageSize();
    }
    curMessageBytes += incrementBytes;
    // update total buffered count when add messageList
    producer.increaseBufferedCount(messageList.size(), incrementBytes);

    // notify partitionSender to getUserMessageList
    notifyAll();
  }

  /**
   * return messageList, if not shouldPut, block in this method
   */
  public synchronized List<Message> getMessageList() {
    while (!shouldPut()) {
      try {
        long waitTime = getWaitTime();
        wait(waitTime);
      } catch (InterruptedException e) {
        LOG.error("getUserMessageList for partition: " + partitionId +
            " is interrupt when waiting: " + e.toString());
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("getUserMessageList wake up for partition: " + partitionId);
    }

    List<Message> returnList = new ArrayList<Message>();
    int returnMsgBytes = 0, returnMsgNumber = 0;

    while (!userMessageList.isEmpty() &&
        returnMsgNumber < maxPutMsgNumber && returnMsgBytes < maxPutMsgBytes) {
      UserMessage userMessage = userMessageList.pollLast();
      returnList.add(userMessage.getMessage());
      curMessageBytes -= userMessage.getMessageSize();
      returnMsgBytes += userMessage.getMessageSize();
      returnMsgNumber++;
    }

    // update total buffered count when poll messageList
    producer.decreaseBufferedCount(returnMsgNumber, returnMsgBytes);
    LOG.info("Ready to put message batch: " + returnList.size() +
        " queue size: " + userMessageList.size() + " and curBytes: " +
        curMessageBytes + " for partition: " + partitionId);
    return returnList;
  }

  private synchronized boolean shouldPut() {
    // when TalosProducer is not active;
    if (!producer.isActive()) {
      return true;
    }

    // when we have enough bytes data or enough number data;
    if (curMessageBytes >= maxPutMsgBytes ||
        userMessageList.size() >= maxPutMsgNumber) {
      return true;
    }

    // when there have at least one message and it has exist enough long time;
    if (userMessageList.size() > 0 && (System.currentTimeMillis() -
        userMessageList.peekLast().getTimestamp() >= maxBufferedTime)) {
      return true;
    }

    return false;
  }

  /**
   * Note: wait(0) represents wait infinite until be notified
   * so we wait minimal 1 milli secs when time <= 0
   */
  private synchronized long getWaitTime() {
    if (userMessageList.size() <= 0) {
      return 0;
    }
    long time = userMessageList.peekLast().getTimestamp() + maxBufferedTime -
        System.currentTimeMillis();
    return (time > 0 ? time : 1);
  }
}
