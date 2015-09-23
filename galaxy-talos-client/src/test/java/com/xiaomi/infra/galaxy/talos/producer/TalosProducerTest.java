/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: yongxing@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.producer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Future;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import libthrift091.TException;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.xiaomi.infra.galaxy.talos.admin.TalosAdmin;
import com.xiaomi.infra.galaxy.talos.client.Constants;
import com.xiaomi.infra.galaxy.talos.client.TalosClientConfigKeys;
import com.xiaomi.infra.galaxy.talos.client.TalosClientConfigurationLoader;
import com.xiaomi.infra.galaxy.talos.thrift.ErrorCode;
import com.xiaomi.infra.galaxy.talos.thrift.GalaxyTalosException;
import com.xiaomi.infra.galaxy.talos.thrift.MessageService;
import com.xiaomi.infra.galaxy.talos.thrift.PutMessageRequest;
import com.xiaomi.infra.galaxy.talos.thrift.PutMessageResponse;
import com.xiaomi.infra.galaxy.talos.thrift.Topic;
import com.xiaomi.infra.galaxy.talos.thrift.TopicAttribute;
import com.xiaomi.infra.galaxy.talos.thrift.TopicInfo;
import com.xiaomi.infra.galaxy.talos.thrift.TopicState;
import com.xiaomi.infra.galaxy.talos.thrift.TopicStatus;
import com.xiaomi.infra.galaxy.talos.thrift.TopicTalosResourceName;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

public class TalosProducerTest {
  private static final String base = "abcdefgh ijklmnopqr stuvwxyz 0123456789";
  private static final String resourceName = "12345#TopicName#july777777000999";
  private static final String anotherResourceName = "12345#TopicName#july777777000629";
  private static final String topicName = "TopicName";
  private static final String ownerId = "12345";
  private static final int messageRetentionMs = 1000;
  private static final int partitionNumber = 8;
  private static final int partitionNumber2 = 16;
  private static final int randomStrLen = 15;
  private static final int producerMaxBufferedMillSecs = 10;
  private static final int producerMaxPutMsgNumber = 10;
  private static final int producerMaxPutMsgBytes = 100;
  private static final int checkPartitionInterval = 200;

  private static TalosProducerConfig talosProducerConfig;
  private static TalosProducer talosProducer;
  private static List<ByteBuffer> messageList;
  private static Topic topic;
  private static PutMessageResponse putMessageResponse;

  private static TalosAdmin talosAdminMock;
  private static MessageService.Iface messageClientMock;
  private static volatile int msgPutSuccessCount;
  private static volatile int msgPutFailureCount;

  // generate random string as message for putMessage
  private static String getRandomString(int randomStrLen) {
    Random random = new Random();
    StringBuffer stringBuffer = new StringBuffer();
    for (int i = 0; i < randomStrLen; i++) {
      int number = random.nextInt(base.length());
      stringBuffer.append(base.charAt(number));
    }
    return stringBuffer.toString();
  }

  private static void clearCounter() {
    msgPutFailureCount = 0;
    msgPutSuccessCount = 0;
  }

  private synchronized void addSuccessCounter() {
    msgPutSuccessCount++;
  }

  private synchronized void addFailureCounter() {
    msgPutFailureCount++;
  }

  // define callback for asynchronously putmessage
  FutureCallback<UserMessageResult> testCallback = new FutureCallback<UserMessageResult>() {
    @Override
    public void onSuccess(UserMessageResult result) {
      addSuccessCounter();
    }

    @Override
    public void onFailure(Throwable t) {
      addFailureCounter();
    }
  };

  @Before
  public void setUp() throws TException {
    // set configuration
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
    configuration.setInt(
        TalosClientConfigKeys.GALAXY_TALOS_PRODUCER_CHECK_PARTITION_INTERVAL,
        checkPartitionInterval);
    talosProducerConfig = new TalosProducerConfig(configuration);

    // construct a topic
    TopicInfo topicInfo = new TopicInfo(
        topicName, new TopicTalosResourceName(resourceName), ownerId);
    TopicAttribute topicAttribute = new TopicAttribute()
        .setPartitionNumber(partitionNumber)
        .setMessageRetentionMs(messageRetentionMs);
    TopicState topicState = new TopicState()
        .setTopicStatus(TopicStatus.ACTIVE)
        .setCreateTimestamp(System.currentTimeMillis());
    topic = new Topic(topicInfo, topicAttribute, topicState);

    // mock some return value
    talosAdminMock = Mockito.mock(TalosAdmin.class);
    messageClientMock = Mockito.mock(MessageService.Iface.class);

    // generate 100 random messages
    messageList = new ArrayList<ByteBuffer>();
    for (int i = 0; i < 100; ++i) {
      messageList.add(ByteBuffer.wrap(getRandomString(randomStrLen).getBytes()));
    }

    // mock putMessageResponse
    putMessageResponse = new PutMessageResponse();
    msgPutFailureCount = 0;
    msgPutSuccessCount = 0;
  }

  @After
  public void tearDown() {
  }

  @Test
  public void testBarebonesAddUserMessage() throws Exception {
    when(talosAdminMock.describeTopic(topicName)).thenReturn(topic);
    when(messageClientMock.putMessage(any(PutMessageRequest.class)))
        .thenReturn(putMessageResponse);
    talosProducer = new TalosProducer(talosProducerConfig,
        new TopicTalosResourceName(resourceName),
        talosAdminMock, messageClientMock);

    for (ByteBuffer data : messageList) {
      talosProducer.addUserMessage(data);
    }
    Thread.sleep(producerMaxBufferedMillSecs * 10);
    talosProducer.cancel();
  }

  @Test
  public void testSynchronouslyAddUserMessage() throws Exception {
    when(talosAdminMock.describeTopic(topicName)).thenReturn(topic);
    when(messageClientMock.putMessage(any(PutMessageRequest.class)))
        .thenReturn(putMessageResponse);
    talosProducer = new TalosProducer(talosProducerConfig,
        new TopicTalosResourceName(resourceName),
        talosAdminMock, messageClientMock);

    List<Future<UserMessageResult>> futures =
        new LinkedList<Future<UserMessageResult>>();
    for (ByteBuffer data : messageList) {
      futures.add(talosProducer.addUserMessage(data));
    }
    assertEquals(messageList.size(), futures.size());

    // not need to sleep, just wait to execute finished
    for (Future<UserMessageResult> future : futures) {
      assertEquals(true, future.get().isSuccessful());
    }
    talosProducer.cancel();
  }

  @Test
  public void testAsynchronouslyAddUserMessage() throws Exception {
    when(talosAdminMock.describeTopic(topicName)).thenReturn(topic);
    when(messageClientMock.putMessage(any(PutMessageRequest.class)))
        .thenReturn(putMessageResponse);
    talosProducer = new TalosProducer(talosProducerConfig,
        new TopicTalosResourceName(resourceName),
        talosAdminMock, messageClientMock);

    for (ByteBuffer data : messageList) {
      ListenableFuture<UserMessageResult> future =
          talosProducer.addUserMessage(data);
      Futures.addCallback(future, testCallback);
    }

    // wait for execute finished
    Thread.sleep(producerMaxBufferedMillSecs * 10);
    assertEquals(0, msgPutFailureCount);
    assertEquals(messageList.size(), msgPutSuccessCount);
    clearCounter();
    talosProducer.cancel();
  }

  // addUserMessage partial failure partial success
  @Test
  public void testAddUserMessagePartialFailedPartialSuccess() throws Exception {
    when(talosAdminMock.describeTopic(topicName)).thenReturn(topic);
    when(messageClientMock.putMessage(any(PutMessageRequest.class)))
        .thenThrow(new GalaxyTalosException()
            .setErrorCode(ErrorCode.HBASE_OPERATION_FAILED)
            .setErrMsg("HBase operation failed"))
        .thenReturn(putMessageResponse);
    talosProducer = new TalosProducer(talosProducerConfig,
        new TopicTalosResourceName(resourceName),
        talosAdminMock, messageClientMock);

    List<Future<UserMessageResult>> futures =
        new LinkedList<Future<UserMessageResult>>();
    for (ByteBuffer data : messageList) {
      ListenableFuture<UserMessageResult> future =
          talosProducer.addUserMessage(data);
      Futures.addCallback(future, testCallback);
      futures.add(future);
    }

    // check callback wait for execute finished
    Thread.sleep(producerMaxBufferedMillSecs * 10);
    assertTrue(msgPutFailureCount > 0);
    assertTrue(messageList.size() > msgPutSuccessCount);
    clearCounter();

    // check failed throw GalaxyTalosException
    for (Future<UserMessageResult> future : futures) {
      try {
        future.get(); // failure future throw GalaxyTalosException
      } catch (Throwable t) {
        assertTrue(t.getCause() instanceof GalaxyTalosException);
      }
    }
    talosProducer.cancel();
  }

  // addUserMessage check message validity
  @Test(expected = IllegalArgumentException.class)
  public void testAddUserMessageValidity() throws Exception {
    when(talosAdminMock.describeTopic(topicName)).thenReturn(topic);
    talosProducer = new TalosProducer(talosProducerConfig,
        new TopicTalosResourceName(resourceName),
        talosAdminMock, messageClientMock);

    String partitionKey = getRandomString(
        Constants.TALOS_PARTITION_KEY_LENGTH_MAXIMAL + 1);
    talosProducer.addUserMessage(partitionKey, "sequenceNumber",
        ByteBuffer.wrap("hello".getBytes()));
  }

  @Test(expected = NullPointerException.class)
  public void testAddUserMessageValidity2() throws Exception {
    when(talosAdminMock.describeTopic(topicName)).thenReturn(topic);
    talosProducer = new TalosProducer(talosProducerConfig,
        new TopicTalosResourceName(resourceName),
        talosAdminMock, messageClientMock);

    talosProducer.addUserMessage(ByteBuffer.wrap(null));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testAddUserMessageValidity3() throws Exception {
    when(talosAdminMock.describeTopic(topicName)).thenReturn(topic);
    talosProducer = new TalosProducer(talosProducerConfig,
        new TopicTalosResourceName(resourceName),
        talosAdminMock, messageClientMock);

    String bigStr = getRandomString(
        Constants.TALOS_SINGLE_MESSAGE_BYTES_MAXIMAL + 1);
    talosProducer.addUserMessage(ByteBuffer.wrap(bigStr.getBytes()));
  }

  // check topic not exist when init Producer
  @Test(expected = GalaxyTalosException.class)
  public void testTopicNotExist() throws Exception {
    doThrow(new GalaxyTalosException().setErrorCode(ErrorCode.TOPIC_NOT_EXIST))
        .when(talosAdminMock).describeTopic(topicName);
    talosProducer = new TalosProducer(talosProducerConfig,
        new TopicTalosResourceName(resourceName),
        talosAdminMock, messageClientMock);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testTopicNotExistForDifferentResourceName() throws Exception {
    when(talosAdminMock.describeTopic(topicName)).thenReturn(topic);
    talosProducer = new TalosProducer(talosProducerConfig,
        new TopicTalosResourceName(anotherResourceName),
        talosAdminMock, messageClientMock);
  }

  // check partition change when producer running
  @Test
  public void testPartitionChangeDuringProducerRunning() throws Exception {
    TopicInfo topicInfo = new TopicInfo(
        topicName, new TopicTalosResourceName(resourceName), ownerId);
    TopicAttribute topicAttribute = new TopicAttribute()
        .setPartitionNumber(partitionNumber2)
        .setMessageRetentionMs(messageRetentionMs);
    TopicState topicState = new TopicState()
        .setTopicStatus(TopicStatus.ACTIVE)
        .setCreateTimestamp(System.currentTimeMillis());
    Topic another = new Topic(topicInfo, topicAttribute, topicState);

    when(talosAdminMock.describeTopic(topicName))
        .thenReturn(topic).thenReturn(another);
    talosProducer = new TalosProducer(talosProducerConfig,
        new TopicTalosResourceName(resourceName),
        talosAdminMock, messageClientMock);

    assertEquals(partitionNumber, talosProducer.getPartitionNumber());
    // wait check partition interval
    Thread.sleep(checkPartitionInterval * 2);
    assertEquals(partitionNumber2, talosProducer.getOutgoingMessageMap().size());
    assertEquals(partitionNumber2, talosProducer.getPartitionNumber());
  }

  // check topic be deleted when producer running
  @Test
  public void testTopicBeDeletedDuringProducerRunning() throws Exception {
    TopicInfo topicInfo = new TopicInfo(
        topicName, new TopicTalosResourceName(anotherResourceName), ownerId);
    TopicAttribute topicAttribute = new TopicAttribute()
        .setPartitionNumber(partitionNumber)
        .setMessageRetentionMs(messageRetentionMs);
    TopicState topicState = new TopicState()
        .setTopicStatus(TopicStatus.ACTIVE)
        .setCreateTimestamp(System.currentTimeMillis());
    Topic another = new Topic(topicInfo, topicAttribute, topicState);

    when(talosAdminMock.describeTopic(topicName))
        .thenReturn(topic).thenReturn(another);
    talosProducer = new TalosProducer(talosProducerConfig,
        new TopicTalosResourceName(resourceName),
        talosAdminMock, messageClientMock);

    // wait check partition interval
    Thread.sleep(checkPartitionInterval);
  }
}
