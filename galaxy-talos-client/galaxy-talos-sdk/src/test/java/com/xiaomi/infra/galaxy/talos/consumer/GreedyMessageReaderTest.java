/**
 * Copyright 2019, Xiaomi.
 * All rights reserved.
 * Author: huyumei@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.consumer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.junit.Test;
import org.junit.Before; 
import org.junit.After;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.infra.galaxy.talos.client.TalosClientConfig;
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
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.inOrder;

public class GreedyMessageReaderTest {
	private static final Logger LOG = LoggerFactory.getLogger(TalosMessageReaderTest.class);
	private static Properties properties = new Properties();

	private TalosConsumerConfig consumerConfig;
	private static final String topicName = "MyTopic";
	private static final String resourceName = "12345#MyTopic#34595fkdiso456i390";
	private static final int partitionId = 7;
	//private static final String consumerGroup = "MyGroup";
	private static final String workerId = "workerId";
	private static final TopicAndPartition topicAndPartition =
			new TopicAndPartition(topicName, new TopicTalosResourceName(resourceName), partitionId);
	private static final long startMessageOffset = 700;

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

	private GreedyMessageReaderTest.TestMessageProcessor messageProcessor = new GreedyMessageReaderTest.TestMessageProcessor();
	private MessageProcessor messageProcessorMock;

	private GreedyMessageReader messageReader;

	@Before
	public void before() throws Exception {
		messageAndOffsetList = new ArrayList<MessageAndOffset>();

		MessageAndOffset messageAndOffset1 = new MessageAndOffset(new Message(
				ByteBuffer.wrap("message1".getBytes())), startMessageOffset);
		MessageAndOffset messageAndOffset2 = new MessageAndOffset(new Message(
				ByteBuffer.wrap("message2".getBytes())), startMessageOffset + 1);
		MessageAndOffset messageAndOffset3 = new MessageAndOffset(new Message(
				ByteBuffer.wrap("message3".getBytes())), startMessageOffset + 2);
		messageAndOffsetList.add(messageAndOffset1);
		messageAndOffsetList.add(messageAndOffset2);
		messageAndOffsetList.add(messageAndOffset3);

		consumerClientMock = Mockito.mock(ConsumerService.Iface.class);
		simpleConsumerMock = Mockito.mock(SimpleConsumer.class);
	}


	@After
	public void after() throws Exception { }

	@Test
	public void testInitStartOffset() throws Exception {
		LOG.info("[testInitStartOffset] start");

		properties.setProperty(TalosClientConfigKeys.GALAXY_TALOS_SERVICE_ENDPOINT, "testURI");
		consumerConfig = new TalosConsumerConfig(properties, false);

		messageProcessorMock = Mockito.mock(MessageProcessor.class);

		messageReader = new GreedyMessageReader(consumerConfig);
		messageReader.setSimpleConsumer(simpleConsumerMock)
				.setConsumerClient(consumerClientMock)
				.setMessageProcessor(messageProcessorMock)
				.setTopicAndPartition(topicAndPartition)
				.setWorkerId(workerId);

		InOrder inOrder = inOrder(simpleConsumerMock, consumerClientMock, messageProcessorMock);

		doNothing().when(messageProcessorMock).init(topicAndPartition, startMessageOffset);
		messageReader.initStartOffset();
		long startOffset = messageReader.getStartOffset().get();
		assertEquals(startMessageOffset, startOffset);
		inOrder.verify(messageProcessorMock).init(topicAndPartition, startMessageOffset);

		doReturn(messageAndOffsetList).when(simpleConsumerMock).fetchMessage(startMessageOffset);
		doNothing().when(messageProcessorMock).process(messageAndOffsetList, messageReader);
		messageReader.fetchData();
		inOrder.verify(simpleConsumerMock).fetchMessage(startMessageOffset);
		inOrder.verify(messageProcessorMock).process(messageAndOffsetList, messageReader);

		doNothing().when(messageProcessorMock).shutdown(messageReader);
		messageReader.commitCheckPoint();
		inOrder.verify(messageProcessorMock).shutdown(messageReader);

		LOG.info("[testInitStartOffset] done");
	}

} 
