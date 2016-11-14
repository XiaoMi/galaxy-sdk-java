/**
 * Copyright 2016, Xiaomi.
 * All rights reserved.
 * Author: xiajun@xiaomi.com
 */

package com.xiaomi.infra.galaxy.lcs.log.core.file;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import com.xiaomi.infra.galaxy.lcs.log.core.ILogger;
import com.xiaomi.infra.galaxy.lcs.log.core.transaction.Transaction;
import com.xiaomi.infra.galaxy.talos.client.serialization.MessageSerialization;
import com.xiaomi.infra.galaxy.talos.thrift.Message;

public class LCSFileReader extends Transaction<List<Message>> {
  private ILogger logger;
  private String topicName;
  private String rootFilePath;
  private String topicFilePath;

  private String curFilePath;

  public LCSFileReader(ILogger logger, String topicName, String rootFilePath) {
    this.logger = logger;
    this.topicName = topicName;
    this.rootFilePath = rootFilePath;
  }

  @Override
  protected void doInitTransaction() {
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
  }

  @Override
  protected void doStartTransaction() {
    TreeMap<String, File> fileTreeMap = FileUtils.listFile(topicFilePath, topicName);
    if (!fileTreeMap.isEmpty()) {
      curFilePath = fileTreeMap.firstEntry().getValue().getAbsolutePath();
    } else {
      curFilePath = null;
    }

  }

  @Override
  protected List<Message> doTake() {
    List<Message> messageList = new ArrayList<Message>();

    if (curFilePath == null) {
      return messageList;
    }

    try {
      FileInputStream fileInputStream = new FileInputStream(curFilePath);
      DataInputStream dataInputStream = new DataInputStream(fileInputStream);

      while (true) {
        try {
          Message message = MessageSerialization.deserializeMessage(dataInputStream);
          messageList.add(message);
        } catch (EOFException e) {
          logger.debug("Topic: " + topicName + " read [" + messageList.size() +
              "] messages from file: " + curFilePath);
          break;
        } catch (IOException e) {
          logger.error("Topic: " + topicName + " readMessage failed from " +
              "filePath: " + curFilePath +" after read [" + messageList.size() +
              "] messages, we just skip the other message in this file", e);
        }
      }

    } catch (FileNotFoundException e) {
      logger.error("Topic: " + topicName + " filePath: " + curFilePath + " not exist", e);
      throw new RuntimeException("readMessage from " + curFilePath +
          " while it not exist", e);
    }

    return messageList;
  }

  @Override
  protected void doCommitTransaction() {
    if (curFilePath != null) {
      try {
        FileUtils.deleteFile(curFilePath);
        logger.info("Topic: " + topicName + " delete curFile: " + curFilePath +
            " success after read all it's messages");
      } catch (FileNotFoundException e) {
        logger.error("Topic: " + topicName + " delete curFile: " + curFilePath +
            " while it not exist", e);
      } catch (IOException e) {
        logger.error("Topic: " + topicName + " delete curFile: " + curFilePath +
            " failed, the message in this file will send to talos again", e);
      }
    }

    curFilePath = null;
  }

  @Override
  protected void doRollbackTransaction() {
    if (curFilePath != null) {
      logger.error("Topic: " + topicName + " rollbackTransaction for file: " + curFilePath);
    }
    curFilePath = null;
  }

  @Override
  protected void doCloseTransaction() {
  }

}