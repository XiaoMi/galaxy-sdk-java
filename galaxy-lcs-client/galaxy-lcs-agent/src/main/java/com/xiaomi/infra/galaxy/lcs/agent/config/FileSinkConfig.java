/**
 * Copyright 2016, Xiaomi.
 * All rights reserved.
 * Author: xiajun@xiaomi.com
 */

package com.xiaomi.infra.galaxy.lcs.agent.config;


import com.xiaomi.infra.galaxy.lcs.agent.model.SinkType;
import com.xiaomi.infra.galaxy.lcs.log.core.utils.ConfigureChecker;

public class FileSinkConfig extends AgentSinkConfig {
  private String rootFilePath;
  private long maxFileNumber;
  private long rotateFileBytes;
  private long rotateFileIntervalMillis;

  public FileSinkConfig() {
    super(SinkType.FILE);
  }

  public String getRootFilePath() {
    return rootFilePath;
  }

  public void setRootFilePath(String rootFilePath) {
    ConfigureChecker.checkConfigureBlank("rootFilePath", rootFilePath);

    this.rootFilePath = rootFilePath;
  }

  public long getRotateFileBytes() {
    return rotateFileBytes;
  }

  public void setRotateFileBytes(long rotateFileBytes) {
    ConfigureChecker.checkConfigureRange("rotateFileBytes", rotateFileBytes, 1L, 1024L * 1024 * 1024);

    this.rotateFileBytes = rotateFileBytes;
  }

  public long getMaxFileNumber() {
    return maxFileNumber;
  }

  public void setMaxFileNumber(long maxFileNumber) {
    ConfigureChecker.checkConfigureRange("maxFileNumber", maxFileNumber, 1L, 1000L * 1000);

    this.maxFileNumber = maxFileNumber;
  }

  public long getRotateFileIntervalMillis() {
    return rotateFileIntervalMillis;
  }

  public void setRotateFileIntervalMillis(long rotateFileIntervalMillis) {
    ConfigureChecker.checkConfigureRange("rotateFileIntervalMillis",
        rotateFileIntervalMillis, 1, Long.MAX_VALUE);

    this.rotateFileIntervalMillis = rotateFileIntervalMillis;
  }

  @Override
  public String toString() {
    return "FileSinkConfig{" +
        "rootFilePath='" + rootFilePath + '\'' +
        ", maxFileNumber=" + maxFileNumber +
        ", rotateFileBytes=" + rotateFileBytes +
        ", rotateFileIntervalMillis=" + rotateFileIntervalMillis +
        '}';
  }
}
