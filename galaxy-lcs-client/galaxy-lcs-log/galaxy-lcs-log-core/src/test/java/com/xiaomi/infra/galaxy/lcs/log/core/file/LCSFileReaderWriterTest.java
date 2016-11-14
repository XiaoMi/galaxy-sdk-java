/**
 * Copyright 2016, Xiaomi.
 * All rights reserved.
 * Author: xiajun@xiaomi.com
 */

package com.xiaomi.infra.galaxy.lcs.log.core.file;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.xiaomi.infra.galaxy.lcs.log.core.ILogger;
import com.xiaomi.infra.galaxy.lcs.log.core.dummy.DummyLogger;
import com.xiaomi.infra.galaxy.talos.client.serialization.MessageSerialization;
import com.xiaomi.infra.galaxy.talos.client.serialization.MessageSerializationFactory;
import com.xiaomi.infra.galaxy.talos.thrift.Message;
import com.xiaomi.infra.galaxy.talos.thrift.MessageType;

import static org.junit.Assert.assertEquals;

public class LCSFileReaderWriterTest {
  private static File path;
  private static String topicName;
  private static String rootFilePath;
  private static long maxFileNumber;
  private static long rotateFileBytes;
  private static long rotateFileIntervalMillis;
  private static Message message;
  private static List<Message> messageList;
  private static int messageNumber;
  private static String topicFilePath;
  private static String tempTopicFilePath;

  private LCSFileReader lcsFileReader;
  private LCSFileWriter lcsFileWriter;
  @BeforeClass
  public static void setUpTestCase() throws Exception {
    topicName = "LCSFileReaderWriterTestTopic";
    path = new File("./target/rootFilePath/");

    rootFilePath = path.getAbsolutePath();
    System.out.print("rootFilePath: " + rootFilePath + "\n");
    maxFileNumber = 1000;
    rotateFileBytes = 10000;
    rotateFileIntervalMillis = 100;
    topicFilePath = FileUtils.formatTopicFilePath(rootFilePath, topicName);
    tempTopicFilePath = FileUtils.formatTempTopicFilePath(topicFilePath);


    message = new Message();
    message.setCreateTimestamp(System.currentTimeMillis());
    message.setMessageType(MessageType.BINARY);
    message.setSequenceNumber("sequenceNumber");
    message.setMessage("message".getBytes());

    messageList = new ArrayList<Message>(1);
    messageList.add(message);

    messageNumber = 100000;
  }

  @AfterClass
  public static void tearDownTestCase() throws Exception {

  }

  @Before
  public void setUp() throws Exception {
    if (!path.exists() && !path.mkdirs()) {
      throw new IOException("Mkdir path: " + path.getPath() + " failed");
    }

    // delete topic file
    String topicFilePath = FileUtils.formatTopicFilePath(rootFilePath, topicName);
    TreeMap<String, File> fileTreeMap = FileUtils.listFile(topicFilePath, topicName);
    for (File file : fileTreeMap.values()) {
      FileUtils.deleteFile(file.getPath());
    }

    // delete topic temp file
    fileTreeMap = FileUtils.listFile(tempTopicFilePath, topicName);
    for (File file : fileTreeMap.values()) {
      FileUtils.deleteFile(file.getPath());
    }

  }

  @After
  public void tearDown() throws Exception {
    // delete topic file
    String topicFilePath = FileUtils.formatTopicFilePath(rootFilePath, topicName);
    TreeMap<String, File> fileTreeMap = FileUtils.listFile(topicFilePath, topicName);
    for (File file : fileTreeMap.values()) {
      FileUtils.deleteFile(file.getPath());
    }

    // delete topic temp file
    String tempTopicFilePath = FileUtils.formatTempTopicFilePath(topicFilePath);
    fileTreeMap = FileUtils.listFile(tempTopicFilePath, topicName);
    for (File file : fileTreeMap.values()) {
      FileUtils.deleteFile(file.getPath());
    }
  }

  @Test
  public void testWriteAndReadMessage() throws Exception {
    lcsFileWriter = new LCSFileWriter(new DummyLogger(), topicName, rootFilePath,
        maxFileNumber, rotateFileBytes, rotateFileIntervalMillis);
    lcsFileReader = new LCSFileReader(new DummyLogger(), topicName, rootFilePath);

    // write message;
    lcsFileWriter.init();
    for (int i = 0; i < messageNumber; ++i) {
      lcsFileWriter.writeMessage(messageList);
    }

    // wait flush last message;
    try {
      Thread.sleep(rotateFileIntervalMillis * 2);
    } catch (InterruptedException e) {
    }
    lcsFileWriter.writeMessage(new ArrayList<Message>());

    lcsFileWriter.close();

    int readMessageNumber = 0;
    lcsFileReader.initTransaction();
    while (true) {
      lcsFileReader.startTransaction();
      List<Message> readMessageList = lcsFileReader.take();
      lcsFileReader.commitTransaction();

      if (readMessageList == null || readMessageList.size() == 0) {
        break;
      }

      readMessageNumber += readMessageList.size();
      System.out.print("readMessageNumber: " + readMessageNumber + "\n");
      for (Message readMessage : readMessageList) {
        assertEquals(readMessage, message);
      }
    }
    lcsFileReader.closeTransaction();

    assertEquals(readMessageNumber, messageNumber);
  }

  @Test
  public void testRotateFileByFileSize() throws Exception {
    lcsFileWriter = new LCSFileWriter(new DummyLogger(), topicName, rootFilePath,
        maxFileNumber, MessageSerialization.getMessageSize(message,
        MessageSerializationFactory.getDefaultMessageVersion()), 100000000);
    lcsFileWriter.init();

    lcsFileWriter.writeMessage(messageList);
    lcsFileWriter.writeMessage(messageList);
    lcsFileWriter.writeMessage(new ArrayList<Message>());
    assertEquals(FileUtils.listFile(topicFilePath, topicName).size(), 2);

    lcsFileWriter.close();
  }

  @Test
  public void testRotateFileByTime() throws Exception {
    lcsFileWriter = new LCSFileWriter(new DummyLogger(), topicName, rootFilePath,
        maxFileNumber, 10000000, 100);
    lcsFileWriter.init();


    lcsFileWriter.writeMessage(messageList);
    try {
      Thread.sleep(100 + 100);
    } catch (InterruptedException e) {
    }

    lcsFileWriter.writeMessage(messageList);
    try {
      Thread.sleep(100 + 100);
    } catch (InterruptedException e) {
    }
    lcsFileWriter.writeMessage(new ArrayList<Message>());
    assertEquals(FileUtils.listFile(topicFilePath, topicName).size(), 2);

    lcsFileWriter.close();
  }

  @Test (expected = IOException.class)
  public void testWriteFileWithTooManyFile() throws Exception {
    lcsFileWriter = new LCSFileWriter(new DummyLogger(), topicName, rootFilePath,
        1, 1, 100000000);
    lcsFileWriter.init();

    lcsFileWriter.writeMessage(messageList);
    lcsFileWriter.writeMessage(messageList);
    assertEquals(FileUtils.listFile(topicFilePath, topicName).size(), 1);

    lcsFileWriter.close();
  }
}

