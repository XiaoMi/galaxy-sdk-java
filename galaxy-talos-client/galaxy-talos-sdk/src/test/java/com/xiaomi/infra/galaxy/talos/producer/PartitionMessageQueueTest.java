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
import com.xiaomi.infra.galaxy.talos.thrift.Message;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;

public class PartitionMessageQueueTest {
  private static final String msgStr = "hello, partitionMessageQueueTest";
  private static final Message msg = new Message(ByteBuffer.wrap(msgStr.getBytes()));

  private static final int partitionId = 7;
  private static final int maxPutMsgNumber = 3;
  private static final int maxBufferedMillSecs = 200;

  private TalosProducer producer;
  private TalosProducerConfig producerConfig;
  private PartitionMessageQueue partitionMessageQueue;
  private UserMessage userMessage1;
  private UserMessage userMessage2;
  private UserMessage userMessage3;
  private List<UserMessage> userMessageList;
  private List<Message> messageList;

  @Before
  public void setUp() {
    Configuration configuration = new Configuration();
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

    producer = Mockito.mock(TalosProducer.class);
    partitionMessageQueue = new PartitionMessageQueue(
        producerConfig, partitionId, producer);
  }

  @After
  public void tearDown() {
  }

  @Test
  public void testAddGetMessageWhenMaxPutNumber() {
    userMessageList = new ArrayList<UserMessage>();
    userMessageList.add(userMessage1);
    userMessageList.add(userMessage2);
    userMessageList.add(userMessage3);

    messageList = new ArrayList<Message>();
    messageList.add(msg);
    messageList.add(msg);
    messageList.add(msg);

    doNothing().when(producer).increaseBufferedCount(3, msgStr.length() * 3);
    doReturn(true).when(producer).isActive();
    doNothing().when(producer).decreaseBufferedCount(3, msgStr.length() * 3);


    partitionMessageQueue.addMessage(userMessageList);
    assertEquals(messageList.size(),
        partitionMessageQueue.getMessageList().size());
    InOrder inOrder = inOrder(producer);
    inOrder.verify(producer, times(1)).increaseBufferedCount(3, msgStr.length() * 3);
    inOrder.verify(producer).isActive();
    inOrder.verify(producer, times(1)).decreaseBufferedCount(3, msgStr.length() * 3);
    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void testAddGetWaitMessageWhenNumberNotEnough() {
    userMessageList = new ArrayList<UserMessage>();
    userMessageList.add(userMessage1);
    userMessageList.add(userMessage2);
    messageList = new ArrayList<Message>();
    messageList.add(msg);
    messageList.add(msg);

    InOrder inOrder = inOrder(producer);


    doNothing().when(producer).increaseBufferedCount(2, msgStr.length() * 2);
    doReturn(true).when(producer).isActive();
    doNothing().when(producer).decreaseBufferedCount(2, msgStr.length() * 2);

    partitionMessageQueue.addMessage(userMessageList);
    // check log has waiting time
    assertEquals(messageList.size(),
        partitionMessageQueue.getMessageList().size());
    inOrder.verify(producer, times(1)).increaseBufferedCount(2, msgStr.length() * 2);
    // first time not return, second return by time;
    inOrder.verify(producer, times(2)).isActive();
    inOrder.verify(producer, times(1)).decreaseBufferedCount(2, msgStr.length() * 2);
    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void testAddGetMessageWhenNotAlive() {
    userMessageList = new ArrayList<UserMessage>();
    userMessageList.add(userMessage1);
    userMessageList.add(userMessage2);
    userMessageList.add(userMessage3);
    messageList = new ArrayList<Message>();
    messageList.add(msg);
    messageList.add(msg);
    messageList.add(msg);

    InOrder inOrder = inOrder(producer);

    doNothing().when(producer).increaseBufferedCount(3, msgStr.length() * 3);
    doReturn(true).doReturn(false).when(producer).isActive();
    doNothing().when(producer).decreaseBufferedCount(3, msgStr.length() * 3);

    partitionMessageQueue.addMessage(userMessageList);
    // check log has waiting time
    assertEquals(messageList.size(),
        partitionMessageQueue.getMessageList().size());
    assertEquals(0,
        partitionMessageQueue.getMessageList().size());

    inOrder.verify(producer, times(1)).increaseBufferedCount(3, msgStr.length() * 3);
    inOrder.verify(producer).isActive();
    inOrder.verify(producer, times(1)).decreaseBufferedCount(3, msgStr.length() * 3);
    inOrder.verify(producer).isActive();
    inOrder.verify(producer, times(1)).decreaseBufferedCount(0, 0);
    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void testAddGetWaitMessageWhenNotAlive() {
    userMessageList = new ArrayList<UserMessage>();
    userMessageList.add(userMessage1);
    userMessageList.add(userMessage2);
    messageList = new ArrayList<Message>();
    messageList.add(msg);
    messageList.add(msg);

    InOrder inOrder = inOrder(producer);


    doNothing().when(producer).increaseBufferedCount(2, msgStr.length() * 2);
    doReturn(true).doReturn(true).doReturn(false).when(producer).isActive();
    doNothing().when(producer).decreaseBufferedCount(2, msgStr.length() * 2);

    partitionMessageQueue.addMessage(userMessageList);
    // check log has waiting time
    assertEquals(messageList.size(),
        partitionMessageQueue.getMessageList().size());
    assertEquals(0,
        partitionMessageQueue.getMessageList().size());

    inOrder.verify(producer, times(1)).increaseBufferedCount(2, msgStr.length() * 2);
    // first time not return, second return by time;
    inOrder.verify(producer, times(2)).isActive();
    inOrder.verify(producer, times(1)).decreaseBufferedCount(2, msgStr.length() * 2);
    inOrder.verify(producer).isActive();
    inOrder.verify(producer, times(1)).decreaseBufferedCount(0, 0);
    inOrder.verifyNoMoreInteractions();
  }
}
