/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: yongxing@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.producer;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class PartitionMessageQueue {
  private LinkedList<MessageAndFuture> messageFutureList;
  private int curMessageBytes;
  private int partitionId;

  private int maxBufferedMsgNumber;
  private int maxBufferedMsgBytes;
  private int maxBufferedTime;
  private int maxPutMsgNumber;
  private int maxPutMsgBytes;

  public PartitionMessageQueue(TalosProducerConfig producerConfig,
      int partitionId) {
    messageFutureList = new LinkedList<MessageAndFuture>();
    curMessageBytes = 0;
    this.partitionId = partitionId;

    maxBufferedMsgNumber = producerConfig.getMaxBufferedMsgNumber();
    maxBufferedMsgBytes = producerConfig.getMaxBufferedMsgBytes();
    maxBufferedTime = producerConfig.getMaxBufferedMsgTime();
    maxPutMsgNumber = producerConfig.getMaxPutMsgNumber();
    maxPutMsgBytes = producerConfig.getMaxPutMsgBytes();
  }

  public synchronized void addMessage(MessageAndFuture messageAndFuture)
      throws ExcessivePendingMessageException {
    if (messageFutureList.size() >= maxBufferedMsgNumber ||
        curMessageBytes >= maxBufferedMsgBytes) {
      throw new ExcessivePendingMessageException("The queue for partition: " +
          partitionId + " has too many pending message waiting for send to server, " +
          "please change partitionKey of message or just wait a moment.");
    }

    messageFutureList.addFirst(messageAndFuture);
    curMessageBytes += messageAndFuture.getMessageSize();
  }

  public synchronized List<MessageAndFuture> getMessageAndFutureList() {
    if (!shouldPut()) {
      return null;
    }

    List<MessageAndFuture> returnList = new ArrayList<MessageAndFuture>();
    int returnMsgBytes = 0, returnMsgNumber = 0;

    while (!messageFutureList.isEmpty() &&
        returnMsgNumber < maxPutMsgNumber && returnMsgBytes < maxPutMsgBytes) {
      MessageAndFuture messageAndFuture = messageFutureList.pollLast();
      returnList.add(messageAndFuture);
      curMessageBytes -= messageAndFuture.getMessageSize();
      returnMsgBytes += messageAndFuture.getMessageSize();
      returnMsgNumber++;
    }
    return returnList;
  }

  private synchronized boolean shouldPut() {
    return curMessageBytes > maxPutMsgBytes ||
        messageFutureList.size() > maxPutMsgNumber ||
        (messageFutureList.size() > 0 && (System.currentTimeMillis()
        - messageFutureList.peekLast().getTimestamp() >= maxBufferedTime));
  }
}
