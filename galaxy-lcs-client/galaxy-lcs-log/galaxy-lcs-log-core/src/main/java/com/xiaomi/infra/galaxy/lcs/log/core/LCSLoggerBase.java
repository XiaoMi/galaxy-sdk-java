/**
 * Copyright 2016, Xiaomi.
 * All rights reserved.
 * Author: xiajun@xiaomi.com
 */

package com.xiaomi.infra.galaxy.lcs.log.core;


import com.xiaomi.infra.galaxy.talos.client.Utils;
import com.xiaomi.infra.galaxy.talos.thrift.Message;
import com.xiaomi.infra.galaxy.talos.thrift.MessageType;

abstract public class LCSLoggerBase {
  private ILog logger;

  public LCSLoggerBase(ILog logger) {
    this.logger = logger;
  }

  public void write(byte[] data) {
    if (data == null) {
      return;
    }

    Message message = new Message();
    message.setCreateTimestamp(System.currentTimeMillis());
    message.setMessageType(MessageType.BINARY);
    message.setMessage(data);

    logger.info(message);
  }

  public void write(Message message) {
    if (message == null || !message.isSetMessage()) {
      return;
    }

    message.setMessageType(MessageType.BINARY);

    if (!message.isSetCreateTimestamp()) {
      message.setCreateTimestamp(System.currentTimeMillis());
    }

    logger.info(message);
  }


}
