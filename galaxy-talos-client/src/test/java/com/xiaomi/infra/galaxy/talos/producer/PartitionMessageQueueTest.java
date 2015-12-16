/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: yongxing@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.producer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import com.xiaomi.infra.galaxy.talos.client.TalosClientConfigKeys;
import com.xiaomi.infra.galaxy.talos.client.TalosClientConfigurationLoader;
import com.xiaomi.infra.galaxy.talos.thrift.Message;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.times;

public class PartitionMessageQueueTest {
  private static final String msgStr = "hello, partitionMessageQueueTest";
  private static final Message msg = new Message(ByteBuffer.wrap(msgStr.getBytes()));

  private static final int partitionId = 7;
  private static final int maxPutMsgNumber = 3;
  private static final int maxBufferedMillSecs = 200;

  private static TalosProducer producer;
  private static TalosProducerConfig producerConfig;
  private static PartitionMessageQueue partitionMessageQueue;
  private static UserMessage userMessage1;
  private static UserMessage userMessage2;
  private static UserMessage userMessage3;
  private static List<UserMessage> messageList;

  @Before
  public void setUp() {
    Configuration configuration = TalosClientConfigurationLoader.getConfiguration();
    configuration.setInt(
        TalosClientConfigKeys.GALAXY_TALOS_PRODUCER_MAX_PUT_MESSAGE_NUMBER,
        maxPutMsgNumber);
    configuration.setInt(
        TalosClientConfigKeys.GALAXY_TALOS_PRODUCER_MAX_BUFFERED_MILLI_SECS,
        maxBufferedMillSecs);
    producerConfig = new TalosProducerConfig(configuration);

    userMessage1 = new UserMessage(msg);
    userMessage2 = new UserMessage(msg);
    userMessage3 = new UserMessage(msg);
  }

  @After
  public void tearDown() {
  }

  @Test
  public void testAddGetMessageWhenMaxPutNumber() {
    messageList = new ArrayList<UserMessage>();
    messageList.add(userMessage1);
    messageList.add(userMessage2);
    messageList.add(userMessage3);

    producer = Mockito.mock(TalosProducer.class);
    doNothing().when(producer).increaseBufferedCount(anyInt(), anyInt());
    doNothing().when(producer).decreaseBufferedCount(anyInt(), anyInt());

    partitionMessageQueue = new PartitionMessageQueue(
        producerConfig, partitionId, producer);
    partitionMessageQueue.addMessage(messageList);
    assertEquals(messageList.size(),
        partitionMessageQueue.getUserMessageList().size());
    InOrder inOrder = inOrder(producer);
    inOrder.verify(producer, times(1)).increaseBufferedCount(anyInt(), anyInt());
    inOrder.verify(producer, times(1)).decreaseBufferedCount(anyInt(), anyInt());
    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void testAddGetWaitMessageWhenNumberNotEnough() {
    messageList = new ArrayList<UserMessage>();
    messageList.add(userMessage1);
    messageList.add(userMessage2);

    producer = Mockito.mock(TalosProducer.class);
    doNothing().when(producer).increaseBufferedCount(anyInt(), anyInt());
    doNothing().when(producer).decreaseBufferedCount(anyInt(), anyInt());

    partitionMessageQueue = new PartitionMessageQueue(
        producerConfig, partitionId, producer);
    partitionMessageQueue.addMessage(messageList);
    // check log has waiting time
    assertEquals(messageList.size(),
        partitionMessageQueue.getUserMessageList().size());
    InOrder inOrder = inOrder(producer);
    inOrder.verify(producer, times(1)).increaseBufferedCount(anyInt(), anyInt());
    inOrder.verify(producer, times(1)).decreaseBufferedCount(anyInt(), anyInt());
    inOrder.verifyNoMoreInteractions();
  }
}
