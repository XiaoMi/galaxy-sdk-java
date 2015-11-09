/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: yongxing@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.producer;

public class BufferedMessageCount {
  private int bufferedMsgNumber;
  private int bufferedMsgBytes;

  public BufferedMessageCount(int bufferedMsgNumber, int bufferedMsgBytes) {
    this.bufferedMsgNumber = bufferedMsgNumber;
    this.bufferedMsgBytes = bufferedMsgBytes;
  }

  public int getBufferedMsgNumber() {
    return bufferedMsgNumber;
  }

  public int getBufferedMsgBytes() {
    return bufferedMsgBytes;
  }
}
