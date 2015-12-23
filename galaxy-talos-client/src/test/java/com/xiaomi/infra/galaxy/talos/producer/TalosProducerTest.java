/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: yongxing@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.producer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import libthrift091.TException;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.xiaomi.infra.galaxy.talos.admin.TalosAdmin;
import com.xiaomi.infra.galaxy.talos.client.Constants;
import com.xiaomi.infra.galaxy.talos.client.SimpleTopicAbnormalCallback;
import com.xiaomi.infra.galaxy.talos.client.TalosClientConfigKeys;
import com.xiaomi.infra.galaxy.talos.thrift.DescribeTopicRequest;
import com.xiaomi.infra.galaxy.talos.thrift.ErrorCode;
import com.xiaomi.infra.galaxy.talos.thrift.GalaxyTalosException;
import com.xiaomi.infra.galaxy.talos.thrift.Message;
import com.xiaomi.infra.galaxy.talos.thrift.Topic;
import com.xiaomi.infra.galaxy.talos.thrift.TopicAttribute;
import com.xiaomi.infra.galaxy.talos.thrift.TopicInfo;
import com.xiaomi.infra.galaxy.talos.thrift.TopicState;
import com.xiaomi.infra.galaxy.talos.thrift.TopicStatus;
import com.xiaomi.infra.galaxy.talos.thrift.TopicTalosResourceName;

import static org.mockito.Matchers.anyListOf;
import static org.mockito.Mockito.doNothing;
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
  private static List<Message> messageList;
  private static Topic topic;

  private static TalosAdmin talosAdminMock;
  private static PartitionSender partitionSenderMock;
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

  private synchronized void addSuccessCounter() {
    msgPutSuccessCount++;
  }

  private synchronized void addFailureCounter() {
    msgPutFailureCount++;
  }

  // define callback for asynchronously putmessage
  private class TestCallback implements UserMessageCallback {

    @Override
    public void onSuccess(UserMessageResult userMessageResult) {
      addSuccessCounter();
    }

    @Override
    public void onError(UserMessageResult userMessageResult) {
      addFailureCounter();
    }
  }

  @Before
  public void setUp() throws TException {
    // set configuration
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
    partitionSenderMock = Mockito.mock(PartitionSender.class);

    // generate 100 random messages
    messageList = new ArrayList<Message>();
    for (int i = 0; i < 100; ++i) {
      messageList.add(new Message(ByteBuffer.wrap(
          getRandomString(randomStrLen).getBytes())));
    }

    // mock putMessageResponse
    msgPutFailureCount = 0;
    msgPutSuccessCount = 0;
  }

  @After
  public void tearDown() {
  }

  @Test
  public void testAsynchronouslyAddUserMessage() throws Exception {
    when(talosAdminMock.describeTopic(new DescribeTopicRequest(topicName))).thenReturn(topic);
    talosProducer = new TalosProducer(talosProducerConfig,
        new TopicTalosResourceName(resourceName), talosAdminMock,
        partitionSenderMock, new SimpleTopicAbnormalCallback(),
        new TestCallback());
    doNothing().when(partitionSenderMock).addMessage(anyListOf(UserMessage.class));

    talosProducer.addUserMessage(messageList);
    // wait for execute finished
    Thread.sleep(producerMaxBufferedMillSecs * 10);
  }

  @Test(expected = ProducerNotActiveException.class)
  public void testProducerNotActiveError() throws Exception {
    TopicInfo topicInfo = new TopicInfo(
        topicName, new TopicTalosResourceName(anotherResourceName), ownerId);
    TopicAttribute topicAttribute = new TopicAttribute()
        .setPartitionNumber(partitionNumber)
        .setMessageRetentionMs(messageRetentionMs);
    TopicState topicState = new TopicState()
        .setTopicStatus(TopicStatus.ACTIVE)
        .setCreateTimestamp(System.currentTimeMillis());
    Topic another = new Topic(topicInfo, topicAttribute, topicState);

    when(talosAdminMock.describeTopic(new DescribeTopicRequest(topicName)))
        .thenReturn(topic).thenReturn(another);
    doNothing().when(partitionSenderMock).cancel(true);
    talosProducer = new TalosProducer(talosProducerConfig,
        new TopicTalosResourceName(resourceName), talosAdminMock,
        partitionSenderMock, new SimpleTopicAbnormalCallback(),
        new TestCallback());

    // wait check partition interval
    Thread.sleep(checkPartitionInterval * 2);

    doNothing().when(partitionSenderMock).addMessage(anyListOf(UserMessage.class));
    talosProducer.addUserMessage(messageList);
  }

  // addUserMessage check message validity
  @Test(expected = IllegalArgumentException.class)
  public void testAddUserMessageValidity() throws Exception {
    when(talosAdminMock.describeTopic(new DescribeTopicRequest(topicName))).thenReturn(topic);
    talosProducer = new TalosProducer(talosProducerConfig,
        new TopicTalosResourceName(resourceName), talosAdminMock,
        partitionSenderMock, new SimpleTopicAbnormalCallback(),
        new TestCallback());

    String partitionKey = getRandomString(
        Constants.TALOS_PARTITION_KEY_LENGTH_MAXIMAL + 1);
    ArrayList<Message> list = new ArrayList<Message>();
    list.add(new Message(
        ByteBuffer.wrap("hello".getBytes())).setPartitionKey(partitionKey));
    talosProducer.addUserMessage(list);
  }

  @Test(expected = NullPointerException.class)
  public void testAddUserMessageValidity2() throws Exception {
    when(talosAdminMock.describeTopic(new DescribeTopicRequest(topicName))).thenReturn(topic);
    talosProducer = new TalosProducer(talosProducerConfig,
        new TopicTalosResourceName(resourceName), talosAdminMock,
        partitionSenderMock, new SimpleTopicAbnormalCallback(),
        new TestCallback());

    ArrayList<Message> list = new ArrayList<Message>();
    list.add(null);
    talosProducer.addUserMessage(list);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testAddUserMessageValidity3() throws Exception {
    when(talosAdminMock.describeTopic(new DescribeTopicRequest(topicName))).thenReturn(topic);
    talosProducer = new TalosProducer(talosProducerConfig,
        new TopicTalosResourceName(resourceName), talosAdminMock,
        partitionSenderMock, new SimpleTopicAbnormalCallback(),
        new TestCallback());

    String bigStr = getRandomString(
        Constants.TALOS_SINGLE_MESSAGE_BYTES_MAXIMAL + 1);
    ArrayList<Message> list = new ArrayList<Message>();
    list.add(new Message(ByteBuffer.wrap(bigStr.getBytes())));
    talosProducer.addUserMessage(list);
  }

  // check topic not exist when init Producer
  @Test(expected = GalaxyTalosException.class)
  public void testTopicNotExist() throws Exception {
    doThrow(new GalaxyTalosException().setErrorCode(ErrorCode.TOPIC_NOT_EXIST))
        .when(talosAdminMock).describeTopic(new DescribeTopicRequest(topicName));
    talosProducer = new TalosProducer(talosProducerConfig,
        new TopicTalosResourceName(resourceName), talosAdminMock,
        partitionSenderMock, new SimpleTopicAbnormalCallback(),
        new TestCallback());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testTopicNotExistForDifferentResourceName() throws Exception {
    when(talosAdminMock.describeTopic(new DescribeTopicRequest(topicName))).thenReturn(topic);
    talosProducer = new TalosProducer(talosProducerConfig,
        new TopicTalosResourceName(anotherResourceName), talosAdminMock,
        partitionSenderMock, new SimpleTopicAbnormalCallback(),
        new TestCallback());
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

    when(talosAdminMock.describeTopic(new DescribeTopicRequest(topicName)))
        .thenReturn(topic).thenReturn(another);
    talosProducer = new TalosProducer(talosProducerConfig,
        new TopicTalosResourceName(resourceName), talosAdminMock,
        partitionSenderMock, new SimpleTopicAbnormalCallback(),
        new TestCallback());

    // wait check partition interval
    Thread.sleep(checkPartitionInterval * 2);
    // check the partition number and outgoingMessageMap changing by log info
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

    when(talosAdminMock.describeTopic(new DescribeTopicRequest(topicName)))
        .thenReturn(topic).thenReturn(another);
    doNothing().when(partitionSenderMock).cancel(true);
    talosProducer = new TalosProducer(talosProducerConfig,
        new TopicTalosResourceName(resourceName), talosAdminMock,
        partitionSenderMock, new SimpleTopicAbnormalCallback(),
        new TestCallback());

    // wait check partition interval
    Thread.sleep(checkPartitionInterval * 2);
  }

}
