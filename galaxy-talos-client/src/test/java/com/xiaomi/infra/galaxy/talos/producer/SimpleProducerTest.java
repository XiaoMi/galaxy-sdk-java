/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: yongxing@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.producer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import libthrift091.TException;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import com.xiaomi.infra.galaxy.talos.thrift.GalaxyTalosException;
import com.xiaomi.infra.galaxy.talos.thrift.Message;
import com.xiaomi.infra.galaxy.talos.thrift.MessageService;
import com.xiaomi.infra.galaxy.talos.thrift.PutMessageRequest;
import com.xiaomi.infra.galaxy.talos.thrift.PutMessageResponse;
import com.xiaomi.infra.galaxy.talos.thrift.TopicAndPartition;
import com.xiaomi.infra.galaxy.talos.thrift.TopicTalosResourceName;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.times;

public class SimpleProducerTest {
  private static final String topicName = "MyTopic";
  private static final String resourceName = "12345#MyTopic#dfi34598dfj4";
  private static final int partitionId = 7;

  private static TalosProducerConfig producerConfig;
  private static MessageService.Iface messageClientMock;
  private static TopicAndPartition topicAndPartition;
  private static List<Message> messageList;
  private static SimpleProducer simpleProducer;

  @Before
  public void setUp() throws IOException {
    Configuration configuration = new Configuration();
    producerConfig = new TalosProducerConfig(configuration);
    messageClientMock = Mockito.mock(MessageService.Iface.class);
    topicAndPartition = new TopicAndPartition(topicName,
        new TopicTalosResourceName(resourceName), partitionId);
    Message message = new Message(ByteBuffer.wrap("hello world".getBytes()));
    messageList = new ArrayList<Message>();
    messageList.add(message);

    simpleProducer = new SimpleProducer(producerConfig,
        topicAndPartition, messageClientMock, new AtomicLong(1));
  }

  @After
  public void tearDown() {

  }

  @Test
  public void testPutMessage() throws TException {
    doReturn(new PutMessageResponse()).when(messageClientMock).putMessage(
        any(PutMessageRequest.class));
    boolean putSuccess = simpleProducer.putMessage(messageList);
    assertTrue(putSuccess);

    InOrder inOrder = inOrder(messageClientMock);
    inOrder.verify(messageClientMock, times(1)).putMessage(
        any(PutMessageRequest.class));
    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void testPutMessageException() throws TException {
    doThrow(new GalaxyTalosException()).doReturn(new PutMessageResponse())
        .when(messageClientMock).putMessage(any(PutMessageRequest.class));
    boolean putSuccess = simpleProducer.putMessage(messageList);
    assertFalse(putSuccess);
    while (!putSuccess) {
      putSuccess = simpleProducer.putMessage(messageList);
      assertTrue(putSuccess);
    }

    InOrder inOrder = inOrder(messageClientMock);
    inOrder.verify(messageClientMock, times(2)).putMessage(
        any(PutMessageRequest.class));
    inOrder.verifyNoMoreInteractions();
  }
}