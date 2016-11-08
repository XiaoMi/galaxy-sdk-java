/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: yongxing@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.producer;

public class BufferedMessageCount {
  private long maxBufferedMsgNumber;
  private long maxBufferedMsgBytes;

  private long bufferedMsgNumber;
  private long bufferedMsgBytes;

  public BufferedMessageCount(long maxBufferedMsgNumber, long maxBufferedMsgBytes) {
    this.maxBufferedMsgNumber = maxBufferedMsgNumber;
    this.maxBufferedMsgBytes = maxBufferedMsgBytes;

    bufferedMsgNumber = 0;
    bufferedMsgBytes = 0;
  }

  synchronized public void increase(long diffBufferedMsgNumber, long diffBufferedMsgBytes) {
    this.bufferedMsgNumber += diffBufferedMsgNumber;
    this.bufferedMsgBytes += diffBufferedMsgBytes;
  }

  synchronized public void descrease(long diffBufferedMsgNumber, long diffBufferedMsgBytes) {
    this.bufferedMsgNumber -= diffBufferedMsgNumber;
    this.bufferedMsgBytes -= diffBufferedMsgBytes;
  }

  synchronized public boolean isEmpty() {
    return bufferedMsgNumber == 0;
  }

  synchronized public boolean isFull() {
    return (bufferedMsgNumber >= maxBufferedMsgNumber ||
        bufferedMsgBytes >= maxBufferedMsgBytes);
  }

  public long getBufferedMsgNumber() {
    return bufferedMsgNumber;
  }

  public long getBufferedMsgBytes() {
    return bufferedMsgBytes;
  }
}
