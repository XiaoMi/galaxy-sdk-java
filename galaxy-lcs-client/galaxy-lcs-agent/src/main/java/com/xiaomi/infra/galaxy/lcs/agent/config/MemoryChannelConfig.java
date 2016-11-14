/**
 * Copyright 2016, Xiaomi.
 * All rights reserved.
 * Author: xiajun@xiaomi.com
 */

package com.xiaomi.infra.galaxy.lcs.agent.config;

import com.xiaomi.infra.galaxy.lcs.agent.model.ChannelType;
import com.xiaomi.infra.galaxy.lcs.log.core.utils.ConfigureChecker;

public class MemoryChannelConfig extends AgentChannelConfig {
  private long maxMessageBufferBytes;
  private long flushMessageBytes;
  private long flushMessageNumber;
  private long flushMessageIntervalMillis;

  public MemoryChannelConfig() {
    super(ChannelType.MEMORY);
  }

  public long getMaxMessageBufferBytes() {
    return maxMessageBufferBytes;
  }

  public void setMaxMessageBufferBytes(long maxMessageBufferBytes) {
    ConfigureChecker.checkConfigureRange("maxMessageBufferBytes",
        maxMessageBufferBytes, 1L * 1024 * 1024, Long.MAX_VALUE);

    this.maxMessageBufferBytes = maxMessageBufferBytes;
  }

  public long getFlushMessageBytes() {
    return flushMessageBytes;
  }

  public void setFlushMessageBytes(long flushMessageBytes) {
    ConfigureChecker.checkConfigureRange("flushMessageBytes",
        flushMessageBytes, 1L, 10L * 1024 * 1024);

    this.flushMessageBytes = flushMessageBytes;
  }

  public long getFlushMessageNumber() {
    return flushMessageNumber;
  }

  public void setFlushMessageNumber(long flushMessageNumber) {
    ConfigureChecker.checkConfigureRange("flushMessageNumber",
        flushMessageNumber, 1, 50000L);

    this.flushMessageNumber = flushMessageNumber;
  }

  public long getFlushMessageIntervalMillis() {
    return flushMessageIntervalMillis;
  }

  public void setFlushMessageIntervalMillis(long flushMessageIntervalMillis) {
    ConfigureChecker.checkConfigureRange("flushMessageIntervalMillis",
        flushMessageIntervalMillis, 1, 10L * 60 * 1000);

    this.flushMessageIntervalMillis = flushMessageIntervalMillis;
  }

  @Override
  public String toString() {
    return "MemoryChannelConfig{" +
        "maxMessageBufferBytes=" + maxMessageBufferBytes +
        ", flushMessageBytes=" + flushMessageBytes +
        ", flushMessageNumber=" + flushMessageNumber +
        ", flushMessageIntervalMillis=" + flushMessageIntervalMillis +
        '}';
  }
}
