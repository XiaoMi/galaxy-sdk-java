/**
 * Copyright 2016, Xiaomi.
 * All rights reserved.
 * Author: xiajun@xiaomi.com
 */

package com.xiaomi.infra.galaxy.lcs.log.core.file;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.TreeMap;

import com.google.common.base.Preconditions;

import com.xiaomi.infra.galaxy.lcs.log.core.ILogger;
import com.xiaomi.infra.galaxy.talos.client.serialization.MessageSerialization;
import com.xiaomi.infra.galaxy.talos.client.serialization.MessageSerializationFactory;
import com.xiaomi.infra.galaxy.talos.thrift.Message;

public class LCSFileWriter {
  private ILogger logger;
  private String topicName;
  private String rootFilePath;
  private long maxFileNumber;
  private long rotateFileBytes;
  private long rotateFileIntervalMillis;

  private String topicFilePath;
  private long curFileNumber;
  private long curFileIndex;
  private String curFilePath;
  private long curFileBytes;
  private long lastRotateFileTimestamp;

  private FileOutputStream fileOutputStream;
  private DataOutputStream dataOutputStream;


  public LCSFileWriter(ILogger logger, String topicName, String rootFilePath,
      long maxFileNumber, long rotateFileBytes, long rotateFileIntervalMillis) {
    this.logger = logger;
    this.topicName = topicName;
    this.rootFilePath = rootFilePath;
    this.maxFileNumber = maxFileNumber;
    this.rotateFileBytes = rotateFileBytes;
    this.rotateFileIntervalMillis = rotateFileIntervalMillis;

  }

  public void init() {
    if (rootFilePath.isEmpty()) {
      throw new RuntimeException("please set rootFilePath");
    }

    if (!rootFilePath.startsWith("/")) {
      throw new RuntimeException("please set rootFilePath as the absolute file path");
    }

    File file = new File(rootFilePath);
    if (!file.exists()) {
      throw new RuntimeException("please make sure rootFilePath: " + rootFilePath + " exist");
    }

    topicFilePath = FileUtils.formatTopicFilePath(rootFilePath, topicName);
    // ensure absoluteFilePath exist;
    file = new File(topicFilePath);
    if (!file.exists()) {
      if (!file.mkdir()) {
        throw new RuntimeException("create dir: " + topicFilePath + " failed");
      }
    }

    String topicTempFilePath = FileUtils.formatTempTopicFilePath(topicFilePath);
    // ensure absoluteFilePath exist;
    file = new File(topicTempFilePath);
    if (!file.exists()) {
      if (!file.mkdir()) {
        throw new RuntimeException("create dir: " + topicTempFilePath + " failed");
      }
    }

    lastRotateFileTimestamp = 0;
    curFileIndex = -1;
  }

  public void close() {
    try {
      closeCurFile();
      logger.info("Topic: " + topicName + " close curFile:" + curFilePath + " success");
    } catch (Exception e) {
      logger.info("Topic: " + topicName + "close curFile: " + curFilePath + " failed", e);

    }
  }

  /**
   * we can call writeMessage with emptyMessageList in order to rotate file
   * @param messageList
   * @throws java.io.IOException
   */
  public void writeMessage(List<Message> messageList) throws IOException {
    if (shouldRotateFile()) {
      try {
        rotateFile();
      } catch (IOException e) {
        logger.error("Topic: " + topicName + " rotateFile failed", e);
        throw e;
      }
    }

    if (messageList.size() == 0) {
      return;
    }

    for (Message message : messageList) {
      try {
        MessageSerialization.serializeMessage(message, dataOutputStream, MessageSerializationFactory.getDefaultMessageVersion());
      } catch (IOException e) {
        logger.error("Topic: " + topicName + " writeMessage to file: " +
            curFilePath + " failed", e);
        try {
          rotateFile();
        } catch (Exception e1) {
          logger.error("Topic: " + topicName + " rotateFile failed", e);
        }
        throw e;
      }
    }

    try {
      dataOutputStream.flush();
      curFileBytes = dataOutputStream.size();
    } catch (IOException e) {
      logger.error("Topic: " + topicName + " flushMessage to file: " +
          curFilePath + " failed", e);
      try {
        rotateFile();
      } catch (Exception e1) {
        logger.error("Topic: " + topicName + " rotateFile failed", e);
      }
      throw e;
    }
  }


  private boolean shouldRotateFile() {
    if (fileOutputStream == null) {
      return true;
    }

    if (curFileBytes >= rotateFileBytes) {
      return true;
    }

    if (curFileBytes > 0 &&
        System.currentTimeMillis() - lastRotateFileTimestamp >= rotateFileIntervalMillis) {
      return true;
    }

    return false;
  }

  private void rotateFile() throws IOException {
    closeCurFile();

    // rename temp file;
    TreeMap<String, File> fileTreeMap = FileUtils.listFile(FileUtils.formatTempTopicFilePath(topicFilePath), topicName);
    if (fileTreeMap.size() != 0) {
      Preconditions.checkArgument(fileTreeMap.size() == 1,
          "fileTreeMap have more than one data: " + fileTreeMap);
      String tempFilePath = fileTreeMap.lastEntry().getValue().getAbsolutePath();
      String filePath = FileUtils.formatFilePath(topicFilePath, topicName,
          FileUtils.getFileIndexByTempFilePath(tempFilePath));
      FileUtils.renameTempFilePath(tempFilePath, filePath);
    }

    // reset fileNumber and fileIndex;
    fileTreeMap = FileUtils.listFile(topicFilePath, topicName);
    curFileNumber = fileTreeMap.size();
    if (fileTreeMap.size() != 0) {
      String lastFilePath = fileTreeMap.lastEntry().getValue().getAbsolutePath();
      if (curFileIndex == -1) {
        curFileIndex = FileUtils.getFileIndexByFilePath(lastFilePath);
      } else {
        Preconditions.checkArgument(curFileIndex == FileUtils.getFileIndexByFilePath(lastFilePath));
      }
    } else {
      curFileIndex = -1;
    }

    // check weather rotate file;
    if (curFileNumber >= maxFileNumber) {
      throw new IOException("Too many buffer files, curBufferFileNumber: " +
          curFileNumber + ", maxBufferFileNumber: " + maxFileNumber);
    }

    // create cur file
    curFileNumber++;
    curFileIndex++;
    curFilePath = FileUtils.formatTempFilePath(topicFilePath, topicName, curFileIndex);
    FileUtils.createFile(curFilePath);
    fileOutputStream = new FileOutputStream(curFilePath);
    dataOutputStream = new DataOutputStream(fileOutputStream);

    curFileBytes = 0;
    lastRotateFileTimestamp = System.currentTimeMillis();
    logger.info("Topic: " + topicName + " rotate new file: " + curFilePath + ", curFileNumber: " + curFileNumber);
  }

  private void closeCurFile() throws IOException {
    // close cur file;
    if (fileOutputStream != null) {
      fileOutputStream.flush();
      fileOutputStream.close();
      fileOutputStream = null;
      dataOutputStream.close();
      dataOutputStream = null;
    }
  }
}
