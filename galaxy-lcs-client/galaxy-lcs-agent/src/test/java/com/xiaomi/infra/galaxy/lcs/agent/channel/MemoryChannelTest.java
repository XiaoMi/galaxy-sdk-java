/**
 * Copyright 2016, Xiaomi.
 * All rights reserved.
 * Author: xiajun@xiaomi.com
 */

package com.xiaomi.infra.galaxy.lcs.agent.channel;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.xiaomi.infra.galaxy.lcs.agent.config.AgentConfigKey;
import com.xiaomi.infra.galaxy.lcs.agent.config.MemoryChannelConfig;
import com.xiaomi.infra.galaxy.lcs.thrift.ErrorCode;
import com.xiaomi.infra.galaxy.lcs.thrift.GalaxyLCSException;
import com.xiaomi.infra.galaxy.talos.client.serialization.MessageSerialization;
import com.xiaomi.infra.galaxy.talos.client.serialization.MessageSerializationFactory;
import com.xiaomi.infra.galaxy.talos.thrift.Message;
import com.xiaomi.infra.galaxy.talos.thrift.MessageType;

import static org.junit.Assert.assertEquals;

public class MemoryChannelTest {
  private static String topicName;
  private static Message message;
  private static List<Message> messageList;
  private static int messageNumber;

  private MemoryChannel memoryChannel;
  private MemoryChannelConfig memoryChannelConfig;

  @BeforeClass
  public static void setUpTestCase() throws Exception {
    topicName = "MemoryChannelTestTopic";

    message = new Message();
    message.setCreateTimestamp(System.currentTimeMillis());
    message.setMessageType(MessageType.BINARY);
    message.setSequenceNumber("sequenceNumber");
    message.setMessage("message".getBytes());

    messageList = new ArrayList<Message>(1);
    messageList.add(message);

    messageNumber = 100000;
  }

  @Before
  public void setUp() throws Exception {
    memoryChannelConfig = new MemoryChannelConfig();
    memoryChannelConfig.setFlushMessageNumber(
        AgentConfigKey.DEFAULT_MEMORY_CHANNEL_FLUSH_MESSAGE_NUMBER);
    memoryChannelConfig.setFlushMessageIntervalMillis(
        AgentConfigKey.DEFAULT_MEMORY_CHANNEL_FLUSH_MESSAGE_INTERVAL_MILLIS);
    memoryChannelConfig.setFlushMessageBytes(
        AgentConfigKey.DEFAULT_MEMORY_CHANNEL_FLUSH_MESSAGE_BYTES);
    memoryChannelConfig.setMaxMessageBufferBytes(
        AgentConfigKey.DEFAULT_MEMORY_CHANNEL_MAX_MESSAGE_BUFFER_BYTES);
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testPutAndGetMessage() throws Exception {
    memoryChannelConfig.setMaxMessageBufferBytes(
        messageNumber * MessageSerialization.getMessageSize(message,
            MessageSerializationFactory.getDefaultMessageVersion()));
    memoryChannel = new MemoryChannel(topicName, memoryChannelConfig);

    memoryChannel.start();

    // putMessage
    for (int index = 0; index < messageNumber; ++index) {
      memoryChannel.putMessage(messageList);
    }

    memoryChannel.stop();

    int readMessageNumber = 0;
    while (true) {
      memoryChannel.startTransaction();
      List<Message> readMessageList = memoryChannel.take();
      memoryChannel.commitTransaction();
      if (readMessageList.size() == 0) {
        break;
      }

      readMessageNumber += readMessageList.size();

      for (Message readMessage : readMessageList) {
        assertEquals(message, readMessage);
      }
    }

    assertEquals(messageNumber, readMessageNumber);
  }

  @Test
  public void testPutMessageWithBufferFull() throws Exception {
    memoryChannelConfig.setMaxMessageBufferBytes(
        messageNumber * MessageSerialization.getMessageSize(message,
            MessageSerializationFactory.getDefaultMessageVersion()));
    memoryChannel = new MemoryChannel(topicName, memoryChannelConfig);

    memoryChannel.start();

    // putMessage
    for (int index = 0; index < messageNumber; ++index) {
      memoryChannel.putMessage(messageList);
    }

    try {
      memoryChannel.putMessage(messageList);
      throw new RuntimeException("putMessage when BufferFull not throw exception");
    } catch (GalaxyLCSException e) {
      assertEquals(ErrorCode.BUFFER_FULL, e.getErrorCode());
    }

    memoryChannel.stop();
  }

  @Test
  public void testPutMessageAfterClose() throws Exception {
    memoryChannel = new MemoryChannel(topicName, memoryChannelConfig);

    memoryChannel.start();
    memoryChannel.stop();

    try {
      memoryChannel.putMessage(messageList);
      throw new RuntimeException("putMessage when stopped not throw exception");
    } catch (GalaxyLCSException e) {
      assertEquals(ErrorCode.MODULED_STOPED, e.getErrorCode());
    }
  }

  @Test
  public void testGetMessageWithMessageBytes() throws Exception {
    memoryChannelConfig.setFlushMessageBytes(
        MessageSerialization.getMessageSize(message,
            MessageSerializationFactory.getDefaultMessageVersion()));
    memoryChannelConfig.setFlushMessageIntervalMillis(600000);
    memoryChannel = new MemoryChannel(topicName, memoryChannelConfig);

    memoryChannel.start();

    memoryChannel.putMessage(messageList);
    memoryChannel.startTransaction();
    List<Message> readMessageList = memoryChannel.take();
    memoryChannel.commitTransaction();
    assertEquals(1, readMessageList.size());
    assertEquals(message, readMessageList.get(0));

    memoryChannel.stop();
  }

  @Test
  public void testGetMessageWithMessageNumber() throws Exception {
    memoryChannelConfig.setFlushMessageNumber(1);
    memoryChannelConfig.setFlushMessageIntervalMillis(600000);
    memoryChannel = new MemoryChannel(topicName, memoryChannelConfig);

    memoryChannel.start();

    memoryChannel.putMessage(messageList);
    memoryChannel.startTransaction();
    List<Message> readMessageList = memoryChannel.take();
    memoryChannel.commitTransaction();
    assertEquals(1, readMessageList.size());
    assertEquals(message, readMessageList.get(0));

    memoryChannel.stop();
  }

  @Test
  public void testGetMessageWithInterval() throws Exception {
    memoryChannelConfig.setFlushMessageIntervalMillis(100);
    memoryChannel = new MemoryChannel(topicName, memoryChannelConfig);

    memoryChannel.start();

    memoryChannel.putMessage(messageList);

    Thread.sleep(100 * 2);
    memoryChannel.startTransaction();
    List<Message> readMessageList = memoryChannel.take();
    memoryChannel.commitTransaction();
    assertEquals(1, readMessageList.size());
    assertEquals(message, readMessageList.get(0));

    memoryChannel.stop();
  }
}
