/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: yongxing@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.producer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.xiaomi.infra.galaxy.talos.client.TalosClientConfigKeys;
import com.xiaomi.infra.galaxy.talos.client.Utils;
import com.xiaomi.infra.galaxy.talos.thrift.ErrorCode;
import com.xiaomi.infra.galaxy.talos.thrift.GalaxyTalosException;
import com.xiaomi.infra.galaxy.talos.thrift.Message;
import com.xiaomi.infra.galaxy.talos.thrift.MessageService;
import com.xiaomi.infra.galaxy.talos.thrift.PutMessageRequest;
import com.xiaomi.infra.galaxy.talos.thrift.PutMessageResponse;
import com.xiaomi.infra.galaxy.talos.thrift.TopicTalosResourceName;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.when;

public class PartitionSenderTest {
  private static final String resourceName = "12345#TopicName#july777777000999";
  private static final String topicName = "TopicName";
  private static TopicTalosResourceName talosResourceName;
  private static TalosProducerConfig talosProducerConfig;
  private static PartitionSender partitionSender;
  private static MessageService.Iface messageClientMock;
  private static TalosProducer producerMock;
  private static MessageCallback messageCallback;
  private static ExecutorService messageCallbackExecutors;

  private static UserMessage userMessage1;
  private static UserMessage userMessage2;
  private static UserMessage userMessage3;
  private static UserMessage userMessage4;

  private static final Object globalLock = new Object();
  private static final int producerMaxBufferedMillSecs = 10;
  private static final int producerMaxPutMsgNumber = 5;
  private static final int producerMaxPutMsgBytes = 50;
  private static final int producerMaxBufferedMsgNum = 50;
  private static final int partitionId = 0;
  private static final AtomicLong requestId = new AtomicLong(1);
  private static List<UserMessage> userMessageList;
  private static volatile int msgPutSuccessCount;
  private static volatile int msgPutFailureCount;

  private class MessageCallback implements UserMessageCallback {

    @Override
    public void onSuccess(UserMessageResult userMessageResult) {
      msgPutSuccessCount += userMessageResult.getMessageList().size();
    }

    @Override
    public void onError(UserMessageResult userMessageResult) {
      msgPutFailureCount += userMessageResult.getMessageList().size();
    }
  }

  private void clearCounter() {
    msgPutFailureCount = 0;
    msgPutSuccessCount = 0;
  }

  @Before
  public void setUp() {
    Configuration configuration = new Configuration();
    configuration.setInt(
        TalosClientConfigKeys.GALAXY_TALOS_PRODUCER_MAX_BUFFERED_MILLI_SECS,
        producerMaxBufferedMillSecs);
    configuration.setInt(
        TalosClientConfigKeys.GALAXY_TALOS_PRODUCER_MAX_PUT_MESSAGE_NUMBER,
        producerMaxPutMsgNumber);
    configuration.setInt(
        TalosClientConfigKeys.GALAXY_TALOS_PRODUCER_MAX_PUT_MESSAGE_BYTES,
        producerMaxPutMsgBytes);
    configuration.setInt(
        TalosClientConfigKeys.GALAXY_TALOS_PRODUCER_MAX_BUFFERED_MESSAGE_NUMBER,
        producerMaxBufferedMsgNum);
    talosProducerConfig = new TalosProducerConfig(configuration);

    talosResourceName = new TopicTalosResourceName(resourceName);
    messageCallback = new MessageCallback();
    messageCallbackExecutors = Executors.newFixedThreadPool(
        talosProducerConfig.getThreadPoolsize());
    messageClientMock = Mockito.mock(MessageService.Iface.class);
    producerMock = Mockito.mock(TalosProducer.class);


    userMessageList = new ArrayList<UserMessage>();
    userMessage1 = new UserMessage(
        new Message(ByteBuffer.wrap("hello".getBytes())));
    userMessage2 = new UserMessage(
        new Message(ByteBuffer.wrap("world".getBytes())));
    userMessage3 = new UserMessage(
        new Message(ByteBuffer.wrap("nice day".getBytes())));
    userMessage4 = new UserMessage(
        new Message(ByteBuffer.wrap("good guy".getBytes())));
  }

  @After
  public void tearDown() {}

  @Test
  public void testPartitionSenderSuccessPut() throws Exception {
    userMessageList.add(userMessage1);
    userMessageList.add(userMessage2);
    userMessageList.add(userMessage3);
    userMessageList.add(userMessage4);

    doReturn(new PutMessageResponse()).when(messageClientMock).putMessage(any(PutMessageRequest.class));
    doReturn(true).doReturn(false).when(producerMock).isActive();

    partitionSender = new PartitionSender(partitionId, topicName,
        talosResourceName, requestId, Utils.generateClientId(),
        talosProducerConfig, messageClientMock, messageCallback,
        messageCallbackExecutors, globalLock, producerMock);
    int addCount = 50;
    while (addCount-- > 0) {
      partitionSender.addMessage(userMessageList);
    }

    partitionSender.shutdown();
    assertEquals(200, msgPutSuccessCount);
    clearCounter();
  }

  @Test
  public void testPartitionSenderFailedPut() throws Exception {
    userMessageList.add(userMessage1);
    userMessageList.add(userMessage2);
    userMessageList.add(userMessage3);
    userMessageList.add(userMessage4);

    when(messageClientMock.putMessage(any(PutMessageRequest.class)))
        .thenThrow(new GalaxyTalosException().setErrMsg("put failed"));
    doReturn(true).doReturn(false).when(producerMock).isActive();

    partitionSender = new PartitionSender(partitionId, topicName,
        talosResourceName, requestId, Utils.generateClientId(),
        talosProducerConfig, messageClientMock, messageCallback,
        messageCallbackExecutors, globalLock, producerMock);
    partitionSender.addMessage(userMessageList);

    partitionSender.shutdown();
    assertEquals(0, msgPutSuccessCount);
    assertEquals(4, msgPutFailureCount);
    clearCounter();
  }

  @Test
  public void testPartitionQueueWaitToPut() throws Exception {
    userMessageList.add(userMessage1);
    userMessageList.add(userMessage2);

    doReturn(new PutMessageResponse()).when(messageClientMock).putMessage(any(PutMessageRequest.class));
    doReturn(true).doReturn(false).when(producerMock).isActive();

    partitionSender = new PartitionSender(partitionId, topicName,
        talosResourceName, requestId, Utils.generateClientId(),
        talosProducerConfig, messageClientMock, messageCallback,
        messageCallbackExecutors, globalLock, producerMock);
    partitionSender.addMessage(userMessageList);
    partitionSender.shutdown();

    assertEquals(2, msgPutSuccessCount);
    clearCounter();
  }

  @Test
  public void testPartitiionNotServingDelay() throws Exception {
    userMessageList.add(userMessage1);
    userMessageList.add(userMessage2);

    when(messageClientMock.putMessage(any(PutMessageRequest.class)))
        .thenThrow(new GalaxyTalosException().setErrorCode(
                ErrorCode.PARTITION_NOT_SERVING));
    doReturn(true).doReturn(false).when(producerMock).isActive();

    partitionSender = new PartitionSender(partitionId, topicName,
        talosResourceName, requestId, Utils.generateClientId(),
        talosProducerConfig, messageClientMock, messageCallback,
        messageCallbackExecutors, globalLock, producerMock);

    partitionSender.addMessage(userMessageList);
    partitionSender.shutdown();

    assertEquals(2, msgPutFailureCount);
    clearCounter();
  }


}
