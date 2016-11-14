/**
 * Copyright 2016, Xiaomi.
 * All rights reserved.
 * Author: xiajun@xiaomi.com
 */

package com.xiaomi.infra.galaxy.lcs.agent.channel;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.infra.galaxy.lcs.agent.config.AgentChannelConfig;
import com.xiaomi.infra.galaxy.lcs.agent.config.MemoryChannelConfig;
import com.xiaomi.infra.galaxy.lcs.log.core.transaction.Transaction;
import com.xiaomi.infra.galaxy.lcs.thrift.ErrorCode;
import com.xiaomi.infra.galaxy.lcs.thrift.GalaxyLCSException;
import com.xiaomi.infra.galaxy.talos.client.serialization.MessageSerialization;
import com.xiaomi.infra.galaxy.talos.client.serialization.MessageSerializationFactory;
import com.xiaomi.infra.galaxy.talos.producer.BufferedMessageCount;
import com.xiaomi.infra.galaxy.talos.thrift.Message;

public class MemoryChannel extends Transaction<List<Message>> implements AgentChannel  {
  private static final Logger LOG = LoggerFactory.getLogger(MemoryChannel.class);

  private String topicName;
  private MemoryChannelConfig channelConfig;

  private AtomicBoolean running;

  private Queue<Message> messageQueue;
  private BufferedMessageCount bufferedMessageCount;

  private List<Message> takeMessageList;
  private final Object takeMessageLock;
  private long lastTakeMessageTimestamp;


  public MemoryChannel(String topicName, AgentChannelConfig channelConfig) {
    if (!(channelConfig instanceof MemoryChannelConfig)) {
      throw new RuntimeException("Wrong AgentChannelConfig type: " +
          channelConfig.getChannelType() + " for NonMemoryChannel");
    }

    this.topicName = topicName;
    this.channelConfig = (MemoryChannelConfig)channelConfig;
    if (this.channelConfig.getFlushMessageBytes() >= this.channelConfig.getMaxMessageBufferBytes()) {
      this.channelConfig.setFlushMessageBytes(this.channelConfig.getMaxMessageBufferBytes() - 1);
    }

    this.running = new AtomicBoolean(true);

    this.messageQueue = new ConcurrentLinkedDeque<Message>();
    this.bufferedMessageCount = new BufferedMessageCount(Long.MAX_VALUE,
        this.channelConfig.getMaxMessageBufferBytes());

    this.takeMessageList = null;
    this.takeMessageLock = new Object();
    lastTakeMessageTimestamp = System.currentTimeMillis();
  }

  @Override
  public void start() {
    running.set(true);

    super.initTransaction();
  }

  @Override
  public void stop() {
    running.set(false);
    synchronized (takeMessageLock) {
      takeMessageLock.notify();
    }
  }

  @Override
  public void putMessage(List<Message> messageList) throws GalaxyLCSException {
    if (!running.get()) {
      throw new GalaxyLCSException(ErrorCode.MODULED_STOPED).setDetails("MemoryChannel is stopped");
    }

    if (bufferedMessageCount.isFull()) {
      throw new GalaxyLCSException(ErrorCode.BUFFER_FULL).setDetails("Topic: " +
          topicName + " maxBufferMessageBytes: " +
          this.channelConfig.getMaxMessageBufferBytes() +
          ", actualBufferMessageBytes: " + bufferedMessageCount.getBufferedMsgBytes());
    }

    addMessageBufferCount(messageList);
    messageQueue.addAll(messageList);
    synchronized (takeMessageLock) {
      takeMessageLock.notify();
    }
  }

  @Override
  public void initTransaction() {
    throw new RuntimeException("please use start() instead");
  }

  @Override
  public void closeTransaction() {
    throw new RuntimeException("please use stop() instead");
  }

  @Override
  protected void doInitTransaction() {
  }

  @Override
  protected void doStartTransaction() {
  }

  /**
   * return empty messageList means there will never have message again, the
   * caller should exit itself; Or doTake will block until there are some messages;
   * @return
   */
  @Override
  protected List<Message> doTake() {
    if (takeMessageList != null) {
      return takeMessageList;
    }

    // wait until there are some message or should exit;
    while (shouldWait()) {
      long waitTime = getWaitTime();
      LOG.info("wait time : " + waitTime);
      // <0 means we should not wait anymore;
      if (waitTime < 0) {
        break;
      }

      synchronized (takeMessageLock) {
        try {
          takeMessageLock.wait(waitTime);
        } catch (Exception e) {
        }
      }
    }

    // take message from messageQueue;
    int messageNumber = 0;
    int messageBytes = 0;

    takeMessageList = new ArrayList<Message>();
    while (!messageQueue.isEmpty()) {
      Message message = messageQueue.peek();
      messageNumber ++;
      messageBytes += message.getMessage().length;

      if (messageNumber > this.channelConfig.getFlushMessageNumber() ||
          ((!takeMessageList.isEmpty()) && messageBytes > this.channelConfig.getFlushMessageBytes())) {
        break;
      }

      message = messageQueue.poll();
      bufferedMessageCount.descrease(1, MessageSerialization.getMessageSize(
          message, MessageSerializationFactory.getDefaultMessageVersion()));

      takeMessageList.add(message);
    }

    lastTakeMessageTimestamp = System.currentTimeMillis();
    return takeMessageList;
  }

  @Override
  protected void doCommitTransaction() {
    takeMessageList = null;
  }

  @Override
  protected void doRollbackTransaction() {
    addMessageBufferCount(takeMessageList);
  }

  @Override
  protected void doCloseTransaction() {

  }

  boolean shouldWait() {
    if (!running.get()) {
      return false;
    }

    if (bufferedMessageCount.getBufferedMsgNumber() >= this.channelConfig.getFlushMessageNumber() ||
        bufferedMessageCount.getBufferedMsgBytes() >= this.channelConfig.getFlushMessageBytes()) {
      return false;
    }

    if (bufferedMessageCount.getBufferedMsgNumber() != 0 && System.currentTimeMillis() >=
        lastTakeMessageTimestamp + this.channelConfig.getFlushMessageIntervalMillis()) {

      return false;
    }

    return true;
  }

  long getWaitTime() {
    if (bufferedMessageCount.getBufferedMsgNumber() == 0) {
      return 0;
    }

    long waitTime =  this.channelConfig.getFlushMessageIntervalMillis() +
        lastTakeMessageTimestamp - System.currentTimeMillis();
    if (waitTime == 0) {
      waitTime = -1;
    }
    return waitTime;
  }

  private void addMessageBufferCount(List<Message> messageList) {
    for (Message message : messageList) {
      bufferedMessageCount.increase(1, MessageSerialization.getMessageSize(
          message, MessageSerializationFactory.getDefaultMessageVersion()));
    }
  }
}
