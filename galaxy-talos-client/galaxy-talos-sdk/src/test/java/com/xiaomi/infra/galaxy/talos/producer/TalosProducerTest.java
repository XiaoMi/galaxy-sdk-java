/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: yongxing@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.producer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import libthrift091.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import com.xiaomi.infra.galaxy.talos.admin.TalosAdmin;
import com.xiaomi.infra.galaxy.talos.client.Constants;
import com.xiaomi.infra.galaxy.talos.client.SimpleTopicAbnormalCallback;
import com.xiaomi.infra.galaxy.talos.client.TalosClientConfigKeys;
import com.xiaomi.infra.galaxy.talos.client.TalosClientFactory;
import com.xiaomi.infra.galaxy.talos.thrift.DescribeTopicRequest;
import com.xiaomi.infra.galaxy.talos.thrift.ErrorCode;
import com.xiaomi.infra.galaxy.talos.thrift.GalaxyTalosException;
import com.xiaomi.infra.galaxy.talos.thrift.GetDescribeInfoRequest;
import com.xiaomi.infra.galaxy.talos.thrift.GetDescribeInfoResponse;
import com.xiaomi.infra.galaxy.talos.thrift.Message;
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
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
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
  private static final TopicTalosResourceName talosResourceName =
      new TopicTalosResourceName(resourceName);

  private TalosProducerConfig talosProducerConfig;
  private TalosProducer talosProducer;
  private List<Message> messageList;
  private Topic topic;

  private TalosAdmin talosAdminMock;
  private TalosClientFactory talosClientFactoryMock;
  private MessageService.Iface messageClientMock;
  private PartitionSender partitionSenderMock;
  private volatile int msgPutSuccessCount;
  private volatile int msgPutFailureCount;

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

  private synchronized void addSuccessCounter(int counter) {
    msgPutSuccessCount += counter;
  }

  private synchronized void addFailureCounter(int counter) {
    msgPutFailureCount += counter;
  }

  // define callback for asynchronously putmessage
  private class TestCallback implements UserMessageCallback {

    @Override
    public void onSuccess(UserMessageResult userMessageResult) {
      addSuccessCounter(userMessageResult.getMessageList().size());
    }

    @Override
    public void onError(UserMessageResult userMessageResult) {
      addFailureCounter(userMessageResult.getMessageList().size());
    }
  }
  
  @Before
  public void setUp() throws TException {
    // set properties
    Properties properties = new Properties();
    properties.setProperty(
        TalosClientConfigKeys.GALAXY_TALOS_PRODUCER_MAX_BUFFERED_MILLI_SECS,
        String.valueOf(producerMaxBufferedMillSecs));
    properties.setProperty(
        TalosClientConfigKeys.GALAXY_TALOS_PRODUCER_MAX_PUT_MESSAGE_NUMBER,
        String.valueOf(producerMaxPutMsgNumber));
    properties.setProperty(
        TalosClientConfigKeys.GALAXY_TALOS_PRODUCER_MAX_PUT_MESSAGE_BYTES,
        String.valueOf(producerMaxPutMsgBytes));
    properties.setProperty(
        TalosClientConfigKeys.GALAXY_TALOS_PRODUCER_CHECK_PARTITION_INTERVAL,
        String.valueOf(checkPartitionInterval));
    properties.setProperty(
        TalosClientConfigKeys.GALAXY_TALOS_SERVICE_ENDPOINT, "testURI");
    talosProducerConfig = new TalosProducerConfig(properties, false);

    // construct a getDescribeInfoResponse
    GetDescribeInfoResponse getDescribeInfoResponse = new GetDescribeInfoResponse(
        talosResourceName, partitionNumber);

    // mock some return value
    talosAdminMock = Mockito.mock(TalosAdmin.class);
    talosClientFactoryMock = Mockito.mock(TalosClientFactory.class);
    messageClientMock = Mockito.mock(MessageService.Iface.class);
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

    when(talosClientFactoryMock.newMessageClient()).thenReturn(messageClientMock);
    when(messageClientMock.putMessage(any(PutMessageRequest.class))).thenReturn(new PutMessageResponse());
    when(talosAdminMock.getDescribeInfo(any(GetDescribeInfoRequest.class))).thenReturn(
        getDescribeInfoResponse);
  }

  @After
  public void tearDown() {
  }

  @Test
  public void testAsynchronouslyAddUserMessage() throws Exception {
    talosProducer = new TalosProducer(talosProducerConfig,
        talosResourceName, talosAdminMock, talosClientFactoryMock,
        partitionSenderMock, new SimpleTopicAbnormalCallback(),
        new TestCallback());

    talosProducer.addUserMessage(messageList);
    // wait for execute finished
    Thread.sleep(producerMaxBufferedMillSecs * 10);
  }

  @Test(expected = ProducerNotActiveException.class)
  public void testProducerNotActiveError() throws Exception {
    GetDescribeInfoResponse getDescribeInfoResponse = new GetDescribeInfoResponse(
        talosResourceName, partitionNumber);
    GetDescribeInfoResponse getDescribeInfoResponse2 = new GetDescribeInfoResponse(
        new TopicTalosResourceName(anotherResourceName), partitionNumber);
    when(talosAdminMock.getDescribeInfo(new GetDescribeInfoRequest(topicName)))
        .thenReturn(getDescribeInfoResponse).thenReturn(getDescribeInfoResponse2);
    doNothing().when(partitionSenderMock).shutdown();
    talosProducer = new TalosProducer(talosProducerConfig,
        talosResourceName, talosAdminMock, talosClientFactoryMock,
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
    talosProducer = new TalosProducer(talosProducerConfig,
        talosResourceName, talosAdminMock, talosClientFactoryMock,
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
    talosProducer = new TalosProducer(talosProducerConfig,
        talosResourceName, talosAdminMock, talosClientFactoryMock,
        partitionSenderMock, new SimpleTopicAbnormalCallback(),
        new TestCallback());

    ArrayList<Message> list = new ArrayList<Message>();
    list.add(null);
    talosProducer.addUserMessage(list);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testAddUserMessageValidity3() throws Exception {
    talosProducer = new TalosProducer(talosProducerConfig,
        talosResourceName, talosAdminMock, talosClientFactoryMock,
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
        .when(talosAdminMock).getDescribeInfo(new GetDescribeInfoRequest(topicName));
    talosProducer = new TalosProducer(talosProducerConfig,
        talosResourceName, talosAdminMock, talosClientFactoryMock,
        partitionSenderMock, new SimpleTopicAbnormalCallback(),
        new TestCallback());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testTopicNotExistForDifferentResourceName() throws Exception {
    talosProducer = new TalosProducer(talosProducerConfig,
        new TopicTalosResourceName(anotherResourceName), talosAdminMock, talosClientFactoryMock,
        partitionSenderMock, new SimpleTopicAbnormalCallback(),
        new TestCallback());
  }

  // check partition change when producer running
  @Test
  public void testPartitionChangeDuringProducerRunning() throws Exception {
    GetDescribeInfoResponse getDescribeInfoResponse = new GetDescribeInfoResponse(
        talosResourceName, partitionNumber);
    GetDescribeInfoResponse getDescribeInfoResponse2 = new GetDescribeInfoResponse(
        new TopicTalosResourceName(anotherResourceName), partitionNumber);
    when(talosAdminMock.getDescribeInfo(new GetDescribeInfoRequest(topicName)))
        .thenReturn(getDescribeInfoResponse).thenReturn(getDescribeInfoResponse2);

    talosProducer = new TalosProducer(talosProducerConfig,
        talosResourceName, talosAdminMock, talosClientFactoryMock,
        partitionSenderMock, new SimpleTopicAbnormalCallback(),
        new TestCallback());

    // wait check partition interval
    Thread.sleep(checkPartitionInterval * 2);
    // check the partition number and outgoingMessageMap changing by log info
  }

  // check topic be deleted when producer running
  @Test
  public void testTopicBeDeletedDuringProducerRunning() throws Exception {
    GetDescribeInfoResponse getDescribeInfoResponse = new GetDescribeInfoResponse(
        talosResourceName, partitionNumber);
    GetDescribeInfoResponse getDescribeInfoResponse2 = new GetDescribeInfoResponse(
        new TopicTalosResourceName(anotherResourceName), partitionNumber);
    when(talosAdminMock.getDescribeInfo(new GetDescribeInfoRequest(topicName)))
        .thenReturn(getDescribeInfoResponse).thenReturn(getDescribeInfoResponse2);

    doNothing().when(partitionSenderMock).shutdown();
    talosProducer = new TalosProducer(talosProducerConfig,
        talosResourceName, talosAdminMock, talosClientFactoryMock,
        partitionSenderMock, new SimpleTopicAbnormalCallback(),
        new TestCallback());

    // wait check partition interval
    Thread.sleep(checkPartitionInterval * 2);
  }

  @Test
  public void testAddUserMessage() throws Exception {
    when(messageClientMock.putMessage(any(PutMessageRequest.class))).thenReturn(new PutMessageResponse());

    talosProducer = new TalosProducer(talosProducerConfig,
        talosResourceName, talosAdminMock, talosClientFactoryMock,
        partitionSenderMock, new SimpleTopicAbnormalCallback(),
        new TestCallback());

    for (int i = 0; i < 100; ++i) {
      talosProducer.addUserMessage(messageList);
    }

    talosProducer.shutdown();
    assertEquals(100 * messageList.size(), msgPutSuccessCount);

  }

  @Test (expected = AddMessageTimeoutException.class)
  public void testAddUserMessageTimeout() throws Exception {
    talosProducerConfig.setMaxBufferedMsgBytes(0);
    talosProducerConfig.setMaxBufferedMsgNumber(0);

    doReturn(messageClientMock).when(talosClientFactoryMock).newMessageClient();
    doReturn(new PutMessageResponse()).when(messageClientMock).putMessage(any(PutMessageRequest.class));

    talosProducer = new TalosProducer(talosProducerConfig,
        talosResourceName, talosAdminMock, talosClientFactoryMock,
        partitionSenderMock, new SimpleTopicAbnormalCallback(),
        new TestCallback());

    for (int i = 0; i < 100; ++i) {
      talosProducer.addUserMessage(messageList, 100);
    }
    talosProducer.shutdown();
  }
}