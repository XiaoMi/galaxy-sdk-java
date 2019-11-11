/**
 * Copyright 2019, Xiaomi.
 * All rights reserved.
 * Author: huyumei@xiaomi.com
 */
package com.xiaomi.infra.galaxy.talos.consumer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.google.gson.JsonArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.infra.galaxy.talos.client.NamedThreadFactory;
import com.xiaomi.infra.galaxy.talos.thrift.ConsumerService;
import com.xiaomi.infra.galaxy.talos.thrift.MessageOffset;
import com.xiaomi.infra.galaxy.talos.thrift.MessageService;
import com.xiaomi.infra.galaxy.talos.thrift.TopicAndPartition;
import com.xiaomi.infra.galaxy.talos.thrift.TopicTalosResourceName;

public class GreedyPartitionFetcher {
	//state of fetcher
	private enum FETCHER_STATE {
		ACTIVE,
		DISABLED,
		SHUTDOWN,
	}

	private class Fetcher implements Runnable {
		private MessageReader greedyMessageReader;

		private Fetcher(MessageReader greedyMessageReader) {
			this.greedyMessageReader = greedyMessageReader;
			LOG.info("initialize Fetcher for partition: " + partitionId);
		}

		@Override
		public void run() {
			if (isFetcherActive()) {
				try {
					// query start offset to read, if failed, disable fetcher and return;
					greedyMessageReader.initStartOffset();
				} catch (Throwable e) {
					LOG.error("Worker: " + workerId + " query partition offset error: "
							+ "we will skip this partition", e);
					disableFetcher();
					return;
				}
			}

			// reading data
			while (isFetcherActive()) {
				greedyMessageReader.fetchData();
			}

			//fetcher is not active
			greedyMessageReader.cleanReader();
		}
	}

	private static final Logger LOG = LoggerFactory.getLogger(GreedyPartitionFetcher.class);
	private TopicTalosResourceName topicTalosResourceName;
	private int partitionId;
	private String workerId;
	private ConsumerService.Iface consumerClient;
	private ExecutorService singleExecutor;
	private Future fetcherFuture;
	private FETCHER_STATE curState;

	private TopicAndPartition topicAndPartition;
	private SimpleConsumer simpleConsumer;
	private MessageReader messageReader;

	public GreedyPartitionFetcher(String topicName, TopicTalosResourceName topicTalosResourceName,
			int partitionId, TalosConsumerConfig talosConsumerConfig, String workerId,
			ConsumerService.Iface consumerClient, MessageService.Iface messageClient,
			MessageProcessor messageProcessor, MessageReader greedyMessageReader, Long outerCheckPoint) {
		this.topicTalosResourceName = topicTalosResourceName;
		this.partitionId = partitionId;
		this.workerId = workerId;
		this.consumerClient = consumerClient;
		curState = FETCHER_STATE.ACTIVE;
		singleExecutor = Executors.newSingleThreadExecutor(new NamedThreadFactory(
				"greedy-consumer-" + workerId + "-" + topicName + ":" + partitionId));
		topicAndPartition = new TopicAndPartition(topicName, topicTalosResourceName, partitionId);
		simpleConsumer = new SimpleConsumer(talosConsumerConfig, topicAndPartition, messageClient);

		// set greedyMessageReader
		greedyMessageReader.setWorkerId(workerId)
				.setTopicAndPartition(topicAndPartition)
				.setSimpleConsumer(simpleConsumer)
				.initGreedyMetrics(topicAndPartition.getPartitionId())
				.setMessageProcessor(messageProcessor)
				.setConsumerClient(consumerClient)
				.setOuterCheckPoint(outerCheckPoint);
		this.messageReader = greedyMessageReader;

		LOG.info("The PartitionFetcher for topic: " + topicTalosResourceName + " partition: "
				+ partitionId + " init.");
		fetcherFuture = singleExecutor.submit(new Fetcher(greedyMessageReader));
	}

	// for test
	public GreedyPartitionFetcher(String topicName,
			TopicTalosResourceName topicTalosResourceName, int partitionId,
			String workerId, ConsumerService.Iface consumerClient,
			SimpleConsumer simpleConsumer, MessageReader messageReader) {
		this.topicTalosResourceName = topicTalosResourceName;
		this.partitionId = partitionId;
		this.workerId = workerId;
		this.consumerClient = consumerClient;
		this.messageReader = messageReader;
		curState = FETCHER_STATE.ACTIVE;
		singleExecutor = Executors.newSingleThreadExecutor(new NamedThreadFactory(
				"greedy-consumer-" + workerId + "-" + topicName + ":" + partitionId));
		fetcherFuture = null;

		topicAndPartition = new TopicAndPartition(topicName, topicTalosResourceName, partitionId);
		this.simpleConsumer = simpleConsumer;

		messageReader.setWorkerId(workerId)
				.setTopicAndPartition(topicAndPartition)
				.initGreedyMetrics(topicAndPartition.getPartitionId())
				.setSimpleConsumer(simpleConsumer)
				.setConsumerClient(consumerClient);

		// set greedyMessageReader
		this.messageReader = messageReader;

		LOG.info("The PartitionFetcher for topic: " + topicTalosResourceName + " partition: "
				+ partitionId + " init.");
		fetcherFuture = singleExecutor.submit(new Fetcher(messageReader));
	}

	private synchronized FETCHER_STATE getCurState() {
		return curState;
	}

	public synchronized boolean isFetcherActive() {
		return getCurState() == FETCHER_STATE.ACTIVE;
	}

	private synchronized boolean isFetcherShutdown() {
		return getCurState() == FETCHER_STATE.SHUTDOWN;
	}

	private synchronized boolean isFetcherDisabled() {
		return getCurState() == FETCHER_STATE.DISABLED;
	}

	private synchronized void setFetcherState(FETCHER_STATE setState) {
		curState = setState;
	}

	private void disableFetcher() {
		if (!isFetcherActive()) {
			return;
		}
		setFetcherState(FETCHER_STATE.DISABLED);
		messageReader.cleanReader();
		singleExecutor.shutdownNow();
	}

	public synchronized long getCurCheckPoint() {
		if (!isFetcherActive()) {
			return MessageOffset.START_OFFSET.getValue();
		}
		return messageReader.getCurCheckPoint();
	}

	public JsonArray getFalconData() {
		return messageReader.getConsumerMetrics().toJsonData();
	}

	public void shutDown() {
		if (!isFetcherActive()) {
			return;
		}

		if (fetcherFuture != null) {
			LOG.info("worker: " + workerId + " try to shutdown partition: " + partitionId);
			// 'false' means not stop the running task;
			fetcherFuture.cancel(false);
		}

		// wait task quit gracefully: stop reading, commit offset, clean and
		// shutdown
		messageReader.cleanReader();
		LOG.info("The MessageProcessTask for topic: " + topicTalosResourceName +
				" partition: " + partitionId + " is finished");

		singleExecutor.shutdown();

		while (true) {
			try {
				if (singleExecutor.awaitTermination(100, TimeUnit.MILLISECONDS)) {
					break;
				}
			} catch (InterruptedException e) {
				//doNothing
			}
		}
	}
}
