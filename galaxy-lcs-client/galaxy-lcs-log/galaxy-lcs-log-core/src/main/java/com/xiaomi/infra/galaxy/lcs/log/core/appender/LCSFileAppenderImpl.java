/**
 * Copyright 2016, Xiaomi.
 * All rights reserved.
 * Author: xiajun@xiaomi.com
 */

package com.xiaomi.infra.galaxy.lcs.log.core.appender;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.xiaomi.infra.galaxy.lcs.log.core.ILogger;
import com.xiaomi.infra.galaxy.lcs.log.core.LoggerConstants;
import com.xiaomi.infra.galaxy.lcs.log.core.file.LCSFileWriter;
import com.xiaomi.infra.galaxy.talos.thrift.Message;

public class LCSFileAppenderImpl extends LCSAppender {
  private String rootFilePath;
  private long maxFileNumber;
  private long rotateFileBytes;
  private long rotateFileIntervalMillis;

  private List<Message> emptyMessageList;
  private LCSFileWriter talosFileWriter;

  public LCSFileAppenderImpl(ILogger logger) {
    super(logger);
    maxFileNumber = LoggerConstants.DEFAULT_MAX_FILE_NUMBER;
    rotateFileBytes = LoggerConstants.DEFAULT_ROTATE_FILE_BYTES;
    rotateFileIntervalMillis = LoggerConstants.DEFAULT_ROTATE_FILE_INTERVAL_MILLIS;
  }

  public String getRootFilePath() {
    return rootFilePath;
  }

  public void setRootFilePath(String rootFilePath) {
    this.rootFilePath = rootFilePath;
  }

  public long getMaxFileNumber() {
    return maxFileNumber;
  }

  public void setMaxFileNumber(long maxFileNumber) {
    this.maxFileNumber = maxFileNumber;
  }

  public long getRotateFileBytes() {
    return rotateFileBytes;
  }

  public void setRotateFileBytes(long rotateFileBytes) {
    this.rotateFileBytes = rotateFileBytes;
  }

  public long getRotateFileIntervalMillis() {
    return rotateFileIntervalMillis;
  }

  public void setRotateFileIntervalMillis(long rotateFileIntervalMillis) {
    this.rotateFileIntervalMillis = rotateFileIntervalMillis;
  }

  @Override
  protected void doStart() {
    emptyMessageList = new ArrayList<Message>();
    talosFileWriter = new LCSFileWriter(logger, topicName, rootFilePath,
        maxFileNumber, rotateFileBytes, rotateFileIntervalMillis);
    talosFileWriter.init();
  }

  @Override
  public void doClose() {
    if (talosFileWriter != null) {
      talosFileWriter.close();
    }
  }

  @Override
  protected void doPeriodCheck() {
    try {
      talosFileWriter.writeMessage(emptyMessageList);
    } catch (IOException e) {
      logger.error("TalosFileAppender periodCheck failed, " + e.toString());
    }
  }

  @Override
  protected void doFlushMessage(List<Message> messageList) throws Exception {
    talosFileWriter.writeMessage(messageList);
  }
}
