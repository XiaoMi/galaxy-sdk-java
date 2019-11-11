/**
 * Copyright 2019, Xiaomi.
 * All rights reserved.
 * Author: huyumei@xiaomi.com
 */
package com.xiaomi.infra.galaxy.talos.consumer;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.infra.galaxy.talos.client.Utils;
import com.xiaomi.infra.galaxy.talos.thrift.MessageAndOffset;

public class GreedyMessageReader extends MessageReader implements MessageCheckpointer {
	private static final Logger LOG = LoggerFactory.getLogger(GreedyMessageReader.class);

	public GreedyMessageReader(TalosConsumerConfig talosConsumerConfig) {
		super(talosConsumerConfig);
	}

	@Override
	public void initStartOffset() {
		//get last commit offset or init by outer checkpoint
		long readingStartOffset;
		if (outerCheckPoint != null && outerCheckPoint >= 0) {
			readingStartOffset = outerCheckPoint;
			//burn after reading the first time
			outerCheckPoint = null;
		} else {
				readingStartOffset = queryStartOffset();
		}

		// when consumer starting up, checking:
		// 1) whether not exist last commit offset, which means 'readingStartOffset==-1'
		// 2) whether reset offset
		// 3) note that: the priority of 'reset-config' is larger than 'outer-checkPoint'
		if (readingStartOffset == -1 || consumerConfig.isResetOffsetWhenStart()) {
			startOffset.set(consumerConfig.getResetOffsetValueWhenStart());
		} else {
			startOffset.set(readingStartOffset);
		}

		// guarantee lastCommitOffset and finishedOffset correct
		if (startOffset.longValue() > 0) {
			lastCommitOffset = finishedOffset = startOffset.get() - 1;
		}
		LOG.info("Init startOffset: " + startOffset + " lastCommitOffset: " +
				lastCommitOffset + " for partition: " + topicAndPartition);
		messageProcessor.init(topicAndPartition, startOffset.get());
	}

	public long queryStartOffset() {
		String filePath = consumerConfig.getLocalOffsetPath();
		String fileName;
		if (consumerConfig.isSynchronousCheckpoint()){
			fileName = Utils.getFileNameForPartition(filePath, workerId,
					topicAndPartition.getTopicName(), topicAndPartition.getPartitionId());
		} else {
			fileName = Utils.getFileNameForTopic(filePath, workerId,
					topicAndPartition.getTopicName());
		}
		return getStartOffsetFromFile(fileName);
	}

	/** get startOffset from local file:
	 *  local file not exist, means has not commit offset before, return -1;
	 */
	private long getStartOffsetFromFile(String fileName) {
		Map<Integer, Long> checkpointMap = new HashMap<Integer, Long>();
		try {
			checkpointMap = Utils.getOffsetMapFromFile(fileName);
		} catch (IOException e) {
				LOG.warn("Get startOffset from file: " + fileName + "failed!", e);
		}

		return checkpointMap.containsKey(topicAndPartition.getPartitionId()) ?
				checkpointMap.get(topicAndPartition.getPartitionId()) : -1;
	}

	@Override
	public void commitCheckPoint() {
		innerCheckpoint();
		messageProcessor.shutdown(this);
	}

	private void innerCheckpoint() {
		if (consumerConfig.isCheckpointAutoCommit() && !consumerConfig.isSynchronousCheckpoint()) {
			lastCommitOffset = finishedOffset;
			lastCommitTime = System.currentTimeMillis();
		}
	}

	@Override
	public void fetchData() {
		// control fetch qps
		long currentTime = System.currentTimeMillis();
		if (currentTime - lastFetchTime < fetchInterval) {
			try {
				Thread.sleep(lastFetchTime + fetchInterval - currentTime);
			} catch (InterruptedException e) {
				// do nothing
			}
		}

		// fetch data and process them
		try {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Reading message from offset: " + startOffset.get() +
						" of partition: " + topicAndPartition.getPartitionId());
			}
			long startFetchTime = System.currentTimeMillis();
			List<MessageAndOffset> messageList = simpleConsumer.fetchMessage(startOffset.get());
			lastFetchTime = System.currentTimeMillis();
			consumerMetrics.markFetchDuration(lastFetchTime - startFetchTime);
			// return when no message got
			if (messageList == null || messageList.size() == 0) {
				checkAndCommit(false);
				return;
			}

			/**
			 * Note: We guarantee the committed offset must be the messages that
			 * have been processed by user's MessageProcessor;
			 */
			finishedOffset = messageList.get(messageList.size() - 1).getMessageOffset();
			long startProcessTime = System.currentTimeMillis();
			messageProcessor.process(messageList, this);
			consumerMetrics.markProcessDuration(System.currentTimeMillis() - startProcessTime);
			startOffset.set(finishedOffset + 1);
			checkAndCommit(true);
		} catch (Throwable e) {
			consumerMetrics.markFetchOrProcessFailedTimes();
			LOG.error("Error when getting messages from topic: " +
					topicAndPartition.getTopicTalosResourceName() + " partition: " +
					topicAndPartition.getPartitionId(), e);

			processFetchException(e);
			lastFetchTime = System.currentTimeMillis();
		}
	}

	@Override
	public void setOuterCheckPoint(Long outerCheckPoint) {
		super.setOuterCheckPoint(outerCheckPoint);
	}

	@Override
	public boolean checkpoint() {
		LOG.error("Nonsupport for user checkpoint, commit offset failed!");
		return false;
	}

	@Override
	public boolean checkpoint(long messageOffset) {
		LOG.error("Nonsupport for user checkpoint, commit offset failed!");
		return false;
	}

	private void checkAndCommit(boolean isContinuous) {
		if (shouldCommit(isContinuous)) {
			try {
				innerCheckpoint();
			} catch (Exception e) {
				// when commitOffset failed, we just do nothing;
				LOG.error("commit offset error, we skip to it", e);
			}
		}
	}

}
