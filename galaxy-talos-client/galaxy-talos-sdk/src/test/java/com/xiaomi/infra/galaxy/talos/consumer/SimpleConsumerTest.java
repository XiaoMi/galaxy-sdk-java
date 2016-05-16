/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: yongxing@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.consumer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import libthrift091.TException;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import com.xiaomi.infra.galaxy.talos.client.compression.Compression;
import com.xiaomi.infra.galaxy.talos.producer.TalosProducerConfig;
import com.xiaomi.infra.galaxy.talos.thrift.GetMessageRequest;
import com.xiaomi.infra.galaxy.talos.thrift.GetMessageResponse;
import com.xiaomi.infra.galaxy.talos.thrift.Message;
import com.xiaomi.infra.galaxy.talos.thrift.MessageAndOffset;
import com.xiaomi.infra.galaxy.talos.thrift.MessageBlock;
import com.xiaomi.infra.galaxy.talos.thrift.MessageOffset;
import com.xiaomi.infra.galaxy.talos.thrift.MessageService;
import com.xiaomi.infra.galaxy.talos.thrift.TopicAndPartition;
import com.xiaomi.infra.galaxy.talos.thrift.TopicTalosResourceName;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.when;

public class SimpleConsumerTest {
  private static final String topicName = "MyTopic";
  private static final String resourceName = "12345#MyTopic#34595fkdiso456i390";
  private static final int partitionId = 7;
  private static final long startOffset = 0;

  private static TopicAndPartition topicAndPartition;
  private static TalosProducerConfig producerConfig;
  private static TalosConsumerConfig consumerConfig;
  private static MessageService.Iface messageClientMock;
  private static List<Message> messageList;
  private static List<MessageAndOffset> messageAndOffsetList;

  private static SimpleConsumer simpleConsumer;

  @Before
  public void setUp() {
    Configuration configuration = new Configuration();
    producerConfig = new TalosProducerConfig(configuration);
    consumerConfig = new TalosConsumerConfig(configuration, false);
    topicAndPartition = new TopicAndPartition(topicName,
        new TopicTalosResourceName(resourceName), partitionId);
    messageClientMock = Mockito.mock(MessageService.Iface.class);
    simpleConsumer = new SimpleConsumer(consumerConfig,
        topicAndPartition, messageClientMock);
    messageList = new ArrayList<Message>();
    messageAndOffsetList = new ArrayList<MessageAndOffset>();
  }

  @After
  public void tearDown() {

  }

  @Test (expected = IllegalArgumentException.class)
  public void testClientPrefixIllegal() throws Exception {
    SimpleConsumer simpleConsumer1 = new SimpleConsumer(consumerConfig,
        topicAndPartition, messageClientMock, "prefix#illegal");
  }

  @Test
  public void testGetTopicTalosResourcename() {
    assertEquals(new TopicTalosResourceName(resourceName),
        simpleConsumer.getTopicTalosResourceName());
  }

  @Test
  public void testGetPartitionId() {
    assertEquals(partitionId, simpleConsumer.getPartitionId());
  }

  @Test
  public void testGetTopicAndPartition() {
    assertEquals(topicAndPartition, simpleConsumer.getTopicAndPartition());
  }

  @Test
  public void testCheckStartOffset() throws TException, IOException {
    Message message1 = new Message(ByteBuffer.wrap("message1".getBytes()));
    Message message2 = new Message(ByteBuffer.wrap("message2".getBytes()));
    Message message3 = new Message(ByteBuffer.wrap("message3".getBytes()));
    messageList.add(message1);
    messageList.add(message2);
    messageList.add(message3);
    MessageBlock messageBlock = Compression.compress(messageList, producerConfig.getCompressionType());
    messageBlock.setStartMessageOffset(startOffset);
    List<MessageBlock> messageBlockList = new ArrayList<MessageBlock>(1);
    messageBlockList.add(messageBlock);

    MessageAndOffset messageAndOffset1 = new MessageAndOffset(message1, startOffset);
    MessageAndOffset messageAndOffset2 = new MessageAndOffset(message2, startOffset + 1);
    MessageAndOffset messageAndOffset3 = new MessageAndOffset(message3, startOffset + 2);
    messageAndOffsetList.add(messageAndOffset1);
    messageAndOffsetList.add(messageAndOffset2);
    messageAndOffsetList.add(messageAndOffset3);

    GetMessageResponse response = new GetMessageResponse(messageBlockList, 3,
        "testCheckStartOffset");
    when(messageClientMock.getMessage(any(GetMessageRequest.class)))
        .thenReturn(response);
    simpleConsumer.fetchMessage(MessageOffset.START_OFFSET.getValue(), 100);
    simpleConsumer.fetchMessage(MessageOffset.LATEST_OFFSET.getValue(), 100);
    simpleConsumer.fetchMessage(0, 100);
    simpleConsumer.fetchMessage(2, 100);
    simpleConsumer.fetchMessage(-1, 100);
    simpleConsumer.fetchMessage(-2, 100);
    try {
      simpleConsumer.fetchMessage(-3, 100);
      assertTrue("test check startOffset validity error", false);
    } catch (Exception e) {
      assertTrue(e instanceof IllegalArgumentException);
    }
  }

  @Test
  public void testFetchMessage() throws Exception {
    Message message1 = new Message(ByteBuffer.wrap("message1".getBytes()));
    Message message2 = new Message(ByteBuffer.wrap("message2".getBytes()));
    Message message3 = new Message(ByteBuffer.wrap("message3".getBytes()));
    messageList.add(message1);
    messageList.add(message2);
    messageList.add(message3);
    MessageBlock messageBlock = Compression.compress(messageList, producerConfig.getCompressionType());
    messageBlock.setStartMessageOffset(startOffset);
    List<MessageBlock> messageBlockList = new ArrayList<MessageBlock>(1);
    messageBlockList.add(messageBlock);

    MessageAndOffset messageAndOffset1 = new MessageAndOffset(message1, startOffset);
    MessageAndOffset messageAndOffset2 = new MessageAndOffset(message2, startOffset + 1);
    MessageAndOffset messageAndOffset3 = new MessageAndOffset(message3, startOffset + 2);
    messageAndOffsetList.add(messageAndOffset1);
    messageAndOffsetList.add(messageAndOffset2);
    messageAndOffsetList.add(messageAndOffset3);

    GetMessageResponse response = new GetMessageResponse(messageBlockList, 3,
        "testFetchMessageSequenceId");
    when(messageClientMock.getMessage(any(GetMessageRequest.class)))
        .thenReturn(response);

    List<MessageAndOffset> msgList = simpleConsumer.fetchMessage(startOffset);
    assertEquals(messageAndOffsetList, msgList);

    try {
      simpleConsumer.fetchMessage(startOffset, 3000);
      assertTrue("test fetchMessage illegal maxFetchNumber error", false);
    } catch (Exception e) {
      assertTrue(e instanceof IllegalArgumentException);
    }

    InOrder inOrder = inOrder(messageClientMock);
    inOrder.verify(messageClientMock).getMessage(any(GetMessageRequest.class));
  }

  @Test
  public void testFetchMessageWithMiddleOffset() throws Exception {
    Message message1 = new Message(ByteBuffer.wrap("message1".getBytes()));
    Message message2 = new Message(ByteBuffer.wrap("message2".getBytes()));
    Message message3 = new Message(ByteBuffer.wrap("message3".getBytes()));
    messageList.add(message1);
    messageList.add(message2);
    messageList.add(message3);
    MessageBlock messageBlock = Compression.compress(messageList, producerConfig.getCompressionType());
    messageBlock.setStartMessageOffset(startOffset);
    List<MessageBlock> messageBlockList = new ArrayList<MessageBlock>(1);
    messageBlockList.add(messageBlock);

    MessageAndOffset messageAndOffset2 = new MessageAndOffset(message2, startOffset + 1);
    MessageAndOffset messageAndOffset3 = new MessageAndOffset(message3, startOffset + 2);
    messageAndOffsetList.add(messageAndOffset2);
    messageAndOffsetList.add(messageAndOffset3);

    GetMessageResponse response = new GetMessageResponse(messageBlockList, 3,
        "testFetchMessageSequenceId");
    when(messageClientMock.getMessage(any(GetMessageRequest.class)))
        .thenReturn(response);

    List<MessageAndOffset> msgList = simpleConsumer.fetchMessage(startOffset + 1);
    assertEquals(messageAndOffsetList, msgList);

    try {
      simpleConsumer.fetchMessage(startOffset, 3000);
      assertTrue("test fetchMessage illegal maxFetchNumber error", false);
    } catch (Exception e) {
      assertTrue(e instanceof IllegalArgumentException);
    }

    InOrder inOrder = inOrder(messageClientMock);
    inOrder.verify(messageClientMock).getMessage(any(GetMessageRequest.class));
  }
}