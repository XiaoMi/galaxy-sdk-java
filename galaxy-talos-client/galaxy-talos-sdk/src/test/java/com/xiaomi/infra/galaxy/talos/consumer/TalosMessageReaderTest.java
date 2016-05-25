/**
 * Copyright 2016, Xiaomi.
 * All rights reserved.
 * Author: xiajun@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.consumer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.infra.galaxy.talos.client.TalosClientConfigKeys;
import com.xiaomi.infra.galaxy.talos.thrift.CheckPoint;
import com.xiaomi.infra.galaxy.talos.thrift.ConsumerService;
import com.xiaomi.infra.galaxy.talos.thrift.Message;
import com.xiaomi.infra.galaxy.talos.thrift.MessageAndOffset;
import com.xiaomi.infra.galaxy.talos.thrift.QueryOffsetRequest;
import com.xiaomi.infra.galaxy.talos.thrift.QueryOffsetResponse;
import com.xiaomi.infra.galaxy.talos.thrift.TopicAndPartition;
import com.xiaomi.infra.galaxy.talos.thrift.TopicTalosResourceName;
import com.xiaomi.infra.galaxy.talos.thrift.UpdateOffsetRequest;
import com.xiaomi.infra.galaxy.talos.thrift.UpdateOffsetResponse;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.inOrder;

public class TalosMessageReaderTest {
  private static final Logger LOG = LoggerFactory.getLogger(TalosMessageReaderTest.class);
  private static Properties properties = new Properties();

  private TalosConsumerConfig consumerConfig;
  private static final String topicName = "MyTopic";
  private static final String resourceName = "12345#MyTopic#34595fkdiso456i390";
  private static final int partitionId = 7;
  private static final String consumerGroup = "MyConsumerGroup";
  private static final String workerId = "workerId";
  private static final TopicAndPartition topicAndPartition =
      new TopicAndPartition(topicName, new TopicTalosResourceName(resourceName), partitionId);
  private static final long startMessageOffset = 1;

  private static List<MessageAndOffset> messageAndOffsetList;
  private static List<MessageAndOffset> messageAndOffsetList2;

  private static ConsumerService.Iface consumerClientMock;
  private static SimpleConsumer simpleConsumerMock;
  private class TestMessageProcessor implements MessageProcessor {
    private TopicAndPartition topicAndPartition;
    private long messageOffset;


    @Override
    public void init(TopicAndPartition topicAndPartition, long messageOffset) {
      this.topicAndPartition = topicAndPartition;
      this.messageOffset = messageOffset;
    }

    @Override
    public void process(List<MessageAndOffset> messages, MessageCheckpointer messageCheckpointer) {
      assertTrue(messageCheckpointer.checkpoint());
      // checkpoint low messageOffset
      assertFalse(messageCheckpointer.checkpoint(messageOffset));
      // checkpoint high messageOffset
      assertFalse(messageCheckpointer.checkpoint(messages.get(messages.size()-1).getMessageOffset()));
    }

    @Override
    public void shutdown(MessageCheckpointer messageCheckpointer) {
      assertFalse(messageCheckpointer.checkpoint());
    }

    public TopicAndPartition getTopicAndPartition() {
      return topicAndPartition;
    }

    public long getMessageOffset() {
      return messageOffset;
    }
  }

  private TestMessageProcessor messageProcessor = new TestMessageProcessor();
  private MessageProcessor messageProcessorMock;

  private TalosMessageReader messageReader;

  @Before
  public void setUp() throws Exception {

    messageAndOffsetList = new ArrayList<MessageAndOffset>();
    messageAndOffsetList2 = new ArrayList<MessageAndOffset>();

    MessageAndOffset messageAndOffset1 = new MessageAndOffset(new Message(
        ByteBuffer.wrap("message1".getBytes())), startMessageOffset);
    MessageAndOffset messageAndOffset2 = new MessageAndOffset(new Message(
        ByteBuffer.wrap("message2".getBytes())), startMessageOffset + 1);
    MessageAndOffset messageAndOffset3 = new MessageAndOffset(new Message(
        ByteBuffer.wrap("message3".getBytes())), startMessageOffset + 2);
    messageAndOffsetList.add(messageAndOffset1);
    messageAndOffsetList.add(messageAndOffset2);
    messageAndOffsetList.add(messageAndOffset3);
    MessageAndOffset messageAndOffset4 = new MessageAndOffset(new Message(
        ByteBuffer.wrap("message4".getBytes())), startMessageOffset + 3);
    MessageAndOffset messageAndOffset5 = new MessageAndOffset(new Message(
        ByteBuffer.wrap("message5".getBytes())), startMessageOffset + 4);
    messageAndOffsetList2.add(messageAndOffset4);
    messageAndOffsetList2.add(messageAndOffset5);


    consumerClientMock = Mockito.mock(ConsumerService.Iface.class);
    simpleConsumerMock = Mockito.mock(SimpleConsumer.class);
  }

  @Test
  public void testDefaultCheckpointOffset() throws Exception {
    properties.setProperty(TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_CHECKPOINT_MESSAGE_OFFSET, "true");
    properties.setProperty(TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_FETCH_INTERVAL, "0");
    properties.setProperty(TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_COMMIT_OFFSET_INTERVAL, "0");
    properties.setProperty(TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_COMMIT_OFFSET_THRESHOLD, "0");
    consumerConfig = new TalosConsumerConfig(properties, false);

    messageProcessorMock = Mockito.mock(MessageProcessor.class);

    messageReader = new TalosMessageReader(consumerConfig);
    messageReader.setSimpleConsumer(simpleConsumerMock)
        .setConsumerClient(consumerClientMock)
        .setConsumerGroup(consumerGroup)
        .setMessageProcessor(messageProcessorMock)
        .setTopicAndPartition(topicAndPartition)
        .setWorkerId(workerId);



    InOrder inOrder = inOrder(simpleConsumerMock, consumerClientMock, messageProcessorMock);

    QueryOffsetRequest queryOffsetRequest = new QueryOffsetRequest(consumerGroup, topicAndPartition);
    QueryOffsetResponse queryOffsetResponse = new QueryOffsetResponse(startMessageOffset - 1);
    doReturn(queryOffsetResponse).when(consumerClientMock).queryOffset(queryOffsetRequest);
    doNothing().when(messageProcessorMock).init(topicAndPartition, startMessageOffset);
    messageReader.initStartOffset();
    inOrder.verify(consumerClientMock).queryOffset(queryOffsetRequest);
    inOrder.verify(messageProcessorMock).init(topicAndPartition, startMessageOffset);

    doReturn(messageAndOffsetList).when(simpleConsumerMock).fetchMessage(startMessageOffset);
    doNothing().when(messageProcessorMock).process(messageAndOffsetList, messageReader);
    CheckPoint checkpoint = new CheckPoint(consumerGroup, topicAndPartition, startMessageOffset+2, workerId);
    UpdateOffsetRequest updateOffsetRequest = new UpdateOffsetRequest(checkpoint);
    UpdateOffsetResponse updateOffsetResponse = new UpdateOffsetResponse(true);
    doReturn(updateOffsetResponse).when(consumerClientMock).updateOffset(updateOffsetRequest);
    messageReader.fetchData();
    inOrder.verify(simpleConsumerMock).fetchMessage(startMessageOffset);
    inOrder.verify(messageProcessorMock).process(messageAndOffsetList, messageReader);
    inOrder.verify(consumerClientMock).updateOffset(updateOffsetRequest);


    doReturn(messageAndOffsetList2).when(simpleConsumerMock).fetchMessage(startMessageOffset + 3);
    doNothing().when(messageProcessorMock).process(messageAndOffsetList2, messageReader);
    checkpoint = new CheckPoint(consumerGroup, topicAndPartition, startMessageOffset + 4, workerId);
    updateOffsetRequest = new UpdateOffsetRequest(checkpoint);
    updateOffsetResponse = new UpdateOffsetResponse(true);
    doReturn(updateOffsetResponse).when(consumerClientMock).updateOffset(updateOffsetRequest);
    messageReader.fetchData();
    inOrder.verify(simpleConsumerMock).fetchMessage(startMessageOffset + 3);
    inOrder.verify(messageProcessorMock).process(messageAndOffsetList2, messageReader);
    inOrder.verify(consumerClientMock).updateOffset(updateOffsetRequest);


    doNothing().when(messageProcessorMock).shutdown(messageReader);
    messageReader.commitCheckPoint();
    inOrder.verify(messageProcessorMock).shutdown(messageReader);

    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void testUserCheckpointOffset() throws Exception {
    properties.setProperty(TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_CHECKPOINT_MESSAGE_OFFSET, "false");
    properties.setProperty(TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_FETCH_INTERVAL, "0");
    properties.setProperty(TalosClientConfigKeys.GALAXY_TALOS_SERVICE_ENDPOINT, "testURI");
    consumerConfig = new TalosConsumerConfig(properties, false);

    messageReader = new TalosMessageReader(consumerConfig);
    messageReader.setSimpleConsumer(simpleConsumerMock)
        .setConsumerClient(consumerClientMock)
        .setConsumerGroup(consumerGroup)
        .setMessageProcessor(messageProcessor)
        .setTopicAndPartition(topicAndPartition)
        .setWorkerId(workerId);



    InOrder inOrder = inOrder(simpleConsumerMock, consumerClientMock);

    QueryOffsetRequest queryOffsetRequest = new QueryOffsetRequest(consumerGroup, topicAndPartition);
    QueryOffsetResponse queryOffsetResponse = new QueryOffsetResponse(startMessageOffset - 1);
    doReturn(queryOffsetResponse).when(consumerClientMock).queryOffset(queryOffsetRequest);
    messageReader.initStartOffset();
    inOrder.verify(consumerClientMock).queryOffset(queryOffsetRequest);
    assertEquals(messageProcessor.getMessageOffset(), startMessageOffset);
    assertEquals(messageProcessor.getTopicAndPartition(), topicAndPartition);

    doReturn(messageAndOffsetList).when(simpleConsumerMock).fetchMessage(startMessageOffset);
    CheckPoint checkpoint = new CheckPoint(consumerGroup, topicAndPartition, startMessageOffset+2, workerId);
    UpdateOffsetRequest updateOffsetRequest = new UpdateOffsetRequest(checkpoint);
    UpdateOffsetResponse updateOffsetResponse = new UpdateOffsetResponse(true);
    doReturn(updateOffsetResponse).when(consumerClientMock).updateOffset(updateOffsetRequest);
    messageReader.fetchData();
    inOrder.verify(simpleConsumerMock).fetchMessage(startMessageOffset);
    inOrder.verify(consumerClientMock).updateOffset(updateOffsetRequest);


    doReturn(messageAndOffsetList2).when(simpleConsumerMock).fetchMessage(startMessageOffset + 3);
    checkpoint = new CheckPoint(consumerGroup, topicAndPartition, startMessageOffset + 4, workerId);
    updateOffsetRequest = new UpdateOffsetRequest(checkpoint);
    updateOffsetResponse = new UpdateOffsetResponse(true);
    doReturn(updateOffsetResponse).when(consumerClientMock).updateOffset(updateOffsetRequest);
    messageReader.fetchData();
    inOrder.verify(simpleConsumerMock).fetchMessage(startMessageOffset + 3);
    inOrder.verify(consumerClientMock).updateOffset(updateOffsetRequest);


    messageReader.commitCheckPoint();
    inOrder.verifyNoMoreInteractions();
  }
}