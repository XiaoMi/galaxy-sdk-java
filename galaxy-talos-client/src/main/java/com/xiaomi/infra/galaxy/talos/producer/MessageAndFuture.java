/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: yongxing@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.producer;

import com.google.common.util.concurrent.SettableFuture;

import com.xiaomi.infra.galaxy.talos.thrift.Message;

public class MessageAndFuture {
  private Message message;
  private SettableFuture<UserMessageResult> future;
  private long timestamp;
  private int messageSize;

  public MessageAndFuture(Message message) {
    this.message = message;
    future = SettableFuture.create();
    timestamp = System.currentTimeMillis();
    messageSize = message.getMessage().length;
    if (message.getSequenceNumber() != null) {
     messageSize += message.getSequenceNumber().length();
    }
  }

  public Message getMessage() {
    return message;
  }

  public SettableFuture<UserMessageResult> getFuture() {
    return future;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public int getMessageSize() {
    return messageSize;
  }
}
