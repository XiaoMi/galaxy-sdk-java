/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: yongxing@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.producer;

public class BufferedMessageCount {
  private int maxBufferedMsgNumber;
  private int maxBufferedMsgBytes;

  private int bufferedMsgNumber;
  private int bufferedMsgBytes;

  public BufferedMessageCount(int maxBufferedMsgNumber, int maxBufferedMsgBytes) {
    this.maxBufferedMsgNumber = maxBufferedMsgNumber;
    this.maxBufferedMsgBytes = maxBufferedMsgBytes;

    bufferedMsgNumber = 0;
    bufferedMsgBytes = 0;
  }

  synchronized public void increase(int diffBufferedMsgNumber, int diffBufferedMsgBytes) {
    this.bufferedMsgNumber += diffBufferedMsgNumber;
    this.bufferedMsgBytes += diffBufferedMsgBytes;
  }

  synchronized public void descrease(int diffBufferedMsgNumber, int diffBufferedMsgBytes) {
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

  public int getBufferedMsgNumber() {
    return bufferedMsgNumber;
  }

  public int getBufferedMsgBytes() {
    return bufferedMsgBytes;
  }
}
