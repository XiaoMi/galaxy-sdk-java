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

import com.xiaomi.infra.galaxy.talos.client.TalosClientConfigKeys;
import com.xiaomi.infra.galaxy.talos.client.TalosClientConfigurationLoader;
import com.xiaomi.infra.galaxy.talos.thrift.Message;

import static org.junit.Assert.assertEquals;

public class PartitionMessageQueueTest {
  private static TalosProducerConfig talosProducerConfig;
  private static PartitionMessageQueue partitionMessageQueue;

  private static MessageAndFuture messageAndFuture1;
  private static MessageAndFuture messageAndFuture2;
  private static MessageAndFuture messageAndFuture3;
  private static MessageAndFuture messageAndFuture4;
  private static MessageAndFuture messageAndFuture5;


  private static final int producerMaxBufferedMillSecs = 500;
  private static final int producerMaxPutMsgNumber = 2;
  private static final int producerMaxPutMsgBytes = 30;
  private static final int partitionId = 0;
  private static List<MessageAndFuture> messageAndFutureList;

  @Before
  public void setUp() {
    Configuration configuration = TalosClientConfigurationLoader.getConfiguration();
    configuration.setInt(
        TalosClientConfigKeys.GALAXY_TALOS_PRODUCER_MAX_BUFFERED_MILLI_SECS,
        producerMaxBufferedMillSecs);
    configuration.setInt(
        TalosClientConfigKeys.GALAXY_TALOS_PRODUCER_MAX_PUT_MESSAGE_NUMBER,
        producerMaxPutMsgNumber);
    configuration.setInt(
        TalosClientConfigKeys.GALAXY_TALOS_PRODUCER_MAX_PUT_MESSAGE_BYTES,
        producerMaxPutMsgBytes);
    talosProducerConfig = new TalosProducerConfig(configuration);
    partitionMessageQueue = new PartitionMessageQueue(
        talosProducerConfig, partitionId);
    messageAndFutureList = new ArrayList<MessageAndFuture>();

    messageAndFuture1 = new MessageAndFuture(
        new Message(ByteBuffer.wrap("hello".getBytes())));
    messageAndFuture2 = new MessageAndFuture(
        new Message(ByteBuffer.wrap("world".getBytes())));
    messageAndFuture3 = new MessageAndFuture(
        new Message(ByteBuffer.wrap("nice day".getBytes())));
    messageAndFuture4 = new MessageAndFuture(
        new Message(ByteBuffer.wrap("good guy".getBytes())));
    messageAndFuture5 = new MessageAndFuture(new Message(ByteBuffer.wrap(
        "this message is build for exceeding producerMaxPutBytes.".getBytes())));
  }

  @After
  public void tearDown() {}

  @Test
  public void testPartitionMessageQueue() throws Exception {
    assertEquals(null, partitionMessageQueue.getMessageAndFutureList());
    messageAndFutureList.add(messageAndFuture1);
    messageAndFutureList.add(messageAndFuture2);

    // maxPutMsgNumber
    partitionMessageQueue.addMessage(messageAndFuture1);
    partitionMessageQueue.addMessage(messageAndFuture2);
    partitionMessageQueue.addMessage(messageAndFuture3);
    partitionMessageQueue.addMessage(messageAndFuture4);
    assertEquals(messageAndFutureList,
        partitionMessageQueue.getMessageAndFutureList());

    // time not exceed
    messageAndFutureList.clear();
    messageAndFutureList.add(messageAndFuture3);
    messageAndFutureList.add(messageAndFuture4);
    assertEquals(null, partitionMessageQueue.getMessageAndFutureList());

    // time exceed
    Thread.sleep(producerMaxBufferedMillSecs);
    assertEquals(messageAndFutureList,
        partitionMessageQueue.getMessageAndFutureList());

    // maxPutBytes
    assertEquals(null, partitionMessageQueue.getMessageAndFutureList());
    messageAndFutureList.clear();
    messageAndFutureList.add(messageAndFuture5);
    partitionMessageQueue.addMessage(messageAndFuture5);
    assertEquals(messageAndFutureList,
        partitionMessageQueue.getMessageAndFutureList());
  }
}
