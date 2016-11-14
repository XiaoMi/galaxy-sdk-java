/**
 * Copyright 2016, Xiaomi.
 * All rights reserved.
 * Author: xiajun@xiaomi.com
 */

package com.xiaomi.infra.galaxy.lcs.log.core.appender;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.xiaomi.infra.galaxy.lcs.log.core.ILogger;
import com.xiaomi.infra.galaxy.lcs.log.core.LoggerConstants;
import com.xiaomi.infra.galaxy.talos.client.Utils;
import com.xiaomi.infra.galaxy.talos.client.serialization.MessageSerialization;
import com.xiaomi.infra.galaxy.talos.client.serialization.MessageSerializationFactory;
import com.xiaomi.infra.galaxy.talos.producer.BufferedMessageCount;
import com.xiaomi.infra.galaxy.talos.thrift.Message;

abstract public class LCSAppender {
  protected ILogger logger;

  protected String topicName;
  protected long maxBufferMessageBytes;
  protected long maxBufferMessageNumber;
  protected boolean blockWhenBufferFull;
  protected long flushMessageBytes;
  protected long flushMessageNumber;
  protected long flushIntervalMillis;
  protected long periodCheckIntervalMillis;

  private Queue<Message> messageQueue;
  private List<Message> failedMessageList;
  private BufferedMessageCount bufferedMessageCount;
  private long lastFlushMessageTimestamp;
  private long lastPeriodCheckTimestamp;

  private final Object addMessageLock;
  private final Object flushMessageLock;

  private AtomicBoolean running;
  private ScheduledExecutorService scheduledExecutorService;

  public LCSAppender(ILogger logger) {
    this.logger = logger;

    maxBufferMessageBytes = LoggerConstants.DEFAULT_MAX_BUFFER_MESSAGE_BYTES;
    maxBufferMessageNumber = LoggerConstants.DEFAULT_MAX_BUFFER_MESSAGE_NUMBER;
    blockWhenBufferFull = LoggerConstants.DEFAULE_BLOCK_WHEN_BUFFER_FULL;
    flushMessageBytes = LoggerConstants.DEFAULT_FLUSH_MESSAGE_BYTES;
    flushMessageNumber = LoggerConstants.DEFAULT_FLUSH_MESSAGE_NUMBER;
    flushIntervalMillis = LoggerConstants.DEFAULT_FLUSH_MESSAGE_INTERVAL_MILLIS;
    periodCheckIntervalMillis = LoggerConstants.DEFAULT_PERIOD_PERIOD_INTERVAL_MILLIS;

    messageQueue = new ConcurrentLinkedQueue<Message>();
    failedMessageList = null;
    lastFlushMessageTimestamp = System.currentTimeMillis();
    lastPeriodCheckTimestamp = System.currentTimeMillis();

    addMessageLock = new Object();
    flushMessageLock = new Object();

    running = new AtomicBoolean(true);
    scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
  }

  public String getTopicName() {
    return topicName;
  }

  public void setTopicName(String topicName) {
    this.topicName = topicName;
  }

  public long getMaxBufferMessageBytes() {
    return maxBufferMessageBytes;
  }

  public void setMaxBufferMessageBytes(long maxBufferMessageBytes) {
    this.maxBufferMessageBytes = maxBufferMessageBytes;
  }

  public boolean isBlockWhenBufferFull() {
    return blockWhenBufferFull;
  }

  public void setBlockWhenBufferFull(boolean blockWhenBufferFull) {
    this.blockWhenBufferFull = blockWhenBufferFull;
  }

  public long getMaxBufferMessageNumber() {
    return maxBufferMessageNumber;
  }

  public void setMaxBufferMessageNumber(long maxBufferMessageNumber) {
    this.maxBufferMessageNumber = maxBufferMessageNumber;
  }

  public long getFlushMessageBytes() {
    return flushMessageBytes;
  }

  public void setFlushMessageBytes(long flushMessageBytes) {
    this.flushMessageBytes = flushMessageBytes;
  }

  public long getFlushMessageNumber() {
    return flushMessageNumber;
  }

  public void setFlushMessageNumber(long flushMessageNumber) {
    this.flushMessageNumber = flushMessageNumber;
  }

  public long getFlushIntervalMillis() {
    return flushIntervalMillis;
  }

  public void setFlushIntervalMillis(long flushIntervalMillis) {
    this.flushIntervalMillis = flushIntervalMillis;
  }

  public long getPeriodCheckIntervalMillis() {
    return periodCheckIntervalMillis;
  }

  public void setPeriodCheckIntervalMillis(long periodCheckIntervalMillis) {
    this.periodCheckIntervalMillis = periodCheckIntervalMillis;
  }

  public void start() {
    if (topicName == null || topicName.isEmpty()) {
      throw new RuntimeException("please set topicName");
    }

    bufferedMessageCount = new BufferedMessageCount(maxBufferMessageNumber, maxBufferMessageBytes);

    doStart();

    scheduledExecutorService.submit(new Runnable() {
      @Override
      public void run() {
        flushMessage();
      }
    });
  }

  public void close() {
    running.set(false);
    scheduledExecutorService.shutdown();

    synchronized (flushMessageLock) {
      flushMessageLock.notify();
    }

    try {
      scheduledExecutorService.awaitTermination(120, TimeUnit.MINUTES);
    } catch (InterruptedException e) {
    }

    doClose();
  }

  public void addMessage(Message message) {
    if (!running.get()) {
      logger.error("Appender is closed, we will dorp one message for topic: " + topicName);
      return;
    }

    try {
      Utils.checkMessageValidity(message);
    } catch (Exception e) {
      logger.error("Topic: " + topicName + " message invalid, we will drop it", e);
      return;
    }

    while (bufferedMessageCount.isFull()) {
      if (blockWhenBufferFull) {
        try {
          addMessageLock.wait();
        } catch (Exception e) {
        }
      } else {
        // add drop message counter
        logger.error("Topic: " + topicName + " drop one message because of " +
            "buffer full");
        return;
      }
    }

    doAddMessage(message);
  }

  private void doAddMessage(Message message) {
    messageQueue.add(message);
    bufferedMessageCount.increase(1L, MessageSerialization.getMessageSize(
        message, MessageSerializationFactory.getDefaultMessageVersion()));
    synchronized (flushMessageLock) {
      flushMessageLock.notify();
    }
  }

  private void flushMessage() {
    while (true) {
      try {
        if (!shouldFlushMessage()) {
          synchronized (flushMessageLock) {
            try {
              flushMessageLock.wait(getFlushMessageWaitTime());
            } catch (Exception e) {
            }
          }
        }

        periodCheck();

        if (shouldFlushMessage()) {
          List<Message> messageList = getFlushMessage();
          if (messageList.isEmpty()) {
            break;
          }

          logger.debug("Topic: " + topicName + " flush [" + messageList.size() +
              "] messages");
          try {
            doFlushMessage(messageList);
          } catch (Exception e) {
            logger.error("Topic: " + topicName + " flush [" + messageList.size() +
                "] messages failed", e);
            onFlushMessageFailed(messageList);
          }
        }
      } catch (Exception e) {
        logger.error("Topic: " + topicName + " flush message with unexcepted error", e);
      }
    }
  }

  private boolean shouldFlushMessage() {
    if (!running.get()) {
      return true;
    }

    if (failedMessageList != null) {
      return true;
    }

    if (bufferedMessageCount.getBufferedMsgBytes() >= flushMessageBytes) {
      return true;
    }

    if (bufferedMessageCount.getBufferedMsgNumber() >= flushMessageNumber) {
      return true;
    }

    if (!bufferedMessageCount.isEmpty() &&
        System.currentTimeMillis() - lastFlushMessageTimestamp >= flushIntervalMillis) {
      return true;
    }

    return false;
  }

  private List<Message> getFlushMessage() {
    if (failedMessageList != null) {
      List<Message> messageList = failedMessageList;
      failedMessageList = null;
      return messageList;
    }

    long messageNumber = 0;
    long messageBytes = 0;

    List<Message> messageList = new ArrayList<Message>();
    while (!messageQueue.isEmpty()) {
      Message message = messageQueue.peek();
      messageNumber ++;
      messageBytes += message.getMessage().length;

      if (messageNumber > flushMessageNumber ||
          ((!messageList.isEmpty()) && messageBytes > flushMessageBytes)) {
        break;
      }

      message = messageQueue.poll();
      bufferedMessageCount.descrease(1, MessageSerialization.getMessageSize(
          message, MessageSerializationFactory.getDefaultMessageVersion()));

      messageList.add(message);
    }

    lastFlushMessageTimestamp = System.currentTimeMillis();
    synchronized (addMessageLock) {
      addMessageLock.notifyAll();
    }

    return messageList;
  }


  private void onFlushMessageFailed(List<Message> messageList) {
    failedMessageList = messageList;
  }

  private void periodCheck() {
    if (System.currentTimeMillis() - lastPeriodCheckTimestamp >
        periodCheckIntervalMillis) {
      lastPeriodCheckTimestamp = System.currentTimeMillis();
      doPeriodCheck();
    }
  }


  private long getFlushMessageWaitTime() {
    long waitTime = lastPeriodCheckTimestamp +
        periodCheckIntervalMillis - System.currentTimeMillis();
    waitTime = waitTime > 0 ? waitTime : 1;

    if (messageQueue.size() != 0) {
      long anotherWaitTime = lastFlushMessageTimestamp +
          flushIntervalMillis - System.currentTimeMillis();
      waitTime = anotherWaitTime > 0 && anotherWaitTime < waitTime ?
          anotherWaitTime : waitTime;
    }

    return waitTime;
  }

  abstract protected void doStart();
  abstract protected void doFlushMessage(List<Message> messageList) throws Exception;
  abstract protected void doPeriodCheck();
  abstract protected void doClose();
}
