/**
 * Copyright 2016, Xiaomi.
 * All rights reserved.
 * Author: xiajun@xiaomi.com
 */

package com.xiaomi.infra.galaxy.lcs.log.log4j.appender;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;

import com.xiaomi.infra.galaxy.lcs.log.core.ILogger;
import com.xiaomi.infra.galaxy.lcs.log.core.appender.LCSFileAppenderImpl;
import com.xiaomi.infra.galaxy.lcs.log.core.utils.ConfigureChecker;
import com.xiaomi.infra.galaxy.lcs.log.log4j.logger.AppenderLogger;
import com.xiaomi.infra.galaxy.talos.thrift.Message;

public class LCSFileAppender extends LCSLog4jAppender {
  private static final Logger LOG = Logger.getLogger(LCSFileAppender.class);
  private static final ILogger logger = new AppenderLogger(LOG);

  private String rootFilePath;
  private long maxFileNumber;
  private long rotateFileBytes;
  private long rotateFileIntervalMillis;

  private LCSFileAppenderImpl lcsFileAppender;

  public LCSFileAppender() {
    super(logger, new LCSFileAppenderImpl(logger));

    this.lcsFileAppender = (LCSFileAppenderImpl)lcsAppender;
  }

  public String getRootFilePath() {
    return rootFilePath;
  }

  public void setRootFilePath(String rootFilePath) {
    ConfigureChecker.checkConfigureBlank("rootFilePath", rootFilePath);

    this.rootFilePath = rootFilePath;
    lcsFileAppender.setRootFilePath(rootFilePath);
  }

  public long getMaxFileNumber() {
    return maxFileNumber;
  }

  public void setMaxFileNumber(long maxFileNumber) {
    ConfigureChecker.checkConfigureRange("maxFileNumber",
        maxFileNumber, 1, Long.MAX_VALUE);
    this.maxFileNumber = maxFileNumber;
    lcsFileAppender.setMaxFileNumber(maxFileNumber);
  }

  public long getRotateFileBytes() {
    return rotateFileBytes;
  }

  public void setRotateFileBytes(long rotateFileBytes) {
    ConfigureChecker.checkConfigureRange("rotateFileBytes",
        rotateFileBytes, 1L, 10L * 1024 * 1024);

    this.rotateFileBytes = rotateFileBytes;
    lcsFileAppender.setRotateFileBytes(rotateFileBytes);
  }

  public long getRotateFileIntervalMillis() {
    return rotateFileIntervalMillis;
  }

  public void setRotateFileIntervalMillis(long rotateFileIntervalMillis) {
    ConfigureChecker.checkConfigureRange("rotateFileIntervalMillis",
        rotateFileIntervalMillis, 1L, 10L * 60 * 1000);

    this.rotateFileIntervalMillis = rotateFileIntervalMillis;
    lcsFileAppender.setRotateFileIntervalMillis(rotateFileIntervalMillis);
  }
}
