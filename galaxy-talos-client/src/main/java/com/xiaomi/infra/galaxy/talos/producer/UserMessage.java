/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: yongxing@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.producer;

import com.xiaomi.infra.galaxy.talos.thrift.Message;

public class UserMessage {
  private Message message;
  private long timestamp;
  private int messageSize;

  public UserMessage(Message message) {
    this.message = message;
    timestamp = System.currentTimeMillis();
    messageSize = message.getMessage().length;
    if (message.getSequenceNumber() != null) {
     messageSize += message.getSequenceNumber().length();
    }
  }

  public Message getMessage() {
    return message;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public int getMessageSize() {
    return messageSize;
  }
}
