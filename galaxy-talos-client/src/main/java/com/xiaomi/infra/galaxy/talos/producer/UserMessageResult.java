/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: yongxing@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.producer;

import java.util.List;

import com.xiaomi.infra.galaxy.talos.thrift.Message;

public class UserMessageResult {
  private List<Message> messageList;
  private int partitionId;
  private boolean successful;
  private Throwable cause;

  public UserMessageResult(List<Message> messageList, int partitionId) {
    this.messageList = messageList;
    this.partitionId = partitionId;
    this.successful = false;
    this.cause = null;
  }

  public int getPartitionId() {
    return partitionId;
  }

  public boolean isSuccessful() {
    return successful;
  }

  public List<Message> getMessageList() {
    return messageList;
  }

  public Throwable getCause() {
    return cause;
  }

  public UserMessageResult setSuccessful(boolean successful) {
    this.successful = successful;
    return this;
  }

  public UserMessageResult setCause(Throwable cause) {
    this.cause = cause;
    return this;
  }
}
