/**
 * Copyright 2019, Xiaomi.
 * All rights reserved.
 * Author: huyumei@xiaomi.com
 */
package com.xiaomi.infra.codelab.talos;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import libthrift091.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.infra.galaxy.rpc.thrift.Credential;
import com.xiaomi.infra.galaxy.rpc.thrift.UserType;
import com.xiaomi.infra.galaxy.talos.client.SimpleTopicAbnormalCallback;
import com.xiaomi.infra.galaxy.talos.consumer.GreedyConsumer;
import com.xiaomi.infra.galaxy.talos.consumer.GreedyMessageReaderFactory;
import com.xiaomi.infra.galaxy.talos.consumer.MessageCheckpointer;
import com.xiaomi.infra.galaxy.talos.consumer.MessageProcessor;
import com.xiaomi.infra.galaxy.talos.consumer.MessageProcessorFactory;
import com.xiaomi.infra.galaxy.talos.consumer.TalosConsumerConfig;
import com.xiaomi.infra.galaxy.talos.thrift.MessageAndOffset;
import com.xiaomi.infra.galaxy.talos.thrift.TopicAndPartition;

public class GreedyConsumerDemo {
	private static final Logger LOG = LoggerFactory.getLogger(GreedyConsumerDemo.class);

	// callback for consumer to process messages, that is, consuming logic
	private static class MyMessageProcessor implements MessageProcessor {
		@Override
		public void init(TopicAndPartition topicAndPartition, long messageOffset) {

		}

		@Override
		public void process(List<MessageAndOffset> messages, MessageCheckpointer messageCheckpointer) {
			try {
				// add your process logic for 'messages'
				for (MessageAndOffset messageAndOffset : messages) {
					LOG.info("Message content: " + new String(messageAndOffset.getMessage().getMessage()));
				}

				long count = successGetNumber.addAndGet(messages.size());
				LOG.info("Consuming total data so far: " + count);

			} catch (Throwable throwable) {
				LOG.error("process error, ", throwable);
			}
		}

		@Override
		public void shutdown(MessageCheckpointer messageCheckpointer) {

		}
	}


	// using for thread-safe when processing different partition data
	private static class MyMessageProcessorFactory implements MessageProcessorFactory {
		@Override
		public MessageProcessor createProcessor() {
			return new GreedyConsumerDemo.MyMessageProcessor();
		}
	}

	private static final String accessKey = "$your_team_accessKey";
	private static final String accessSecret = "$your_team_accessSecret";
	private static final String topicName = "testTopic";
	private static final AtomicLong successGetNumber = new AtomicLong(0);
	private static final String workerId = "$yourWorkerId";

	private TalosConsumerConfig consumerConfig;
	private Credential credential;
	private GreedyConsumer greedyConsumer;
	private GreedyMessageReaderFactory greedyMessageReaderFactory = new GreedyMessageReaderFactory();

	public GreedyConsumerDemo() {

		Properties properties = new Properties();
		properties.setProperty("galaxy.talos.service.endpoint", "$talosServiceURI");
		consumerConfig = new TalosConsumerConfig(properties);

		// credential
		credential = new Credential();
		credential.setSecretKeyId(accessKey)
				.setSecretKey(accessSecret)
				.setType(UserType.DEV_XIAOMI);
	}

	public void start() throws TException {
		greedyConsumer = new GreedyConsumer(consumerConfig, credential, topicName,
				greedyMessageReaderFactory, new MyMessageProcessorFactory(), new SimpleTopicAbnormalCallback(),
				workerId);

	}

	public static void main(String[] args) throws Exception {
		GreedyConsumerDemo greedyConsumerDemo = new GreedyConsumerDemo();
		greedyConsumerDemo.start();
	}
}
