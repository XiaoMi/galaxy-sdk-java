/**
 * Copyright 2019, Xiaomi.
 * All rights reserved.
 * Author: huyumei@xiaomi.com
 */
package com.xiaomi.infra.galaxy.talos.consumer;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import libthrift091.TException;

import com.xiaomi.infra.galaxy.lcs.common.logger.Slf4jLogger;
import com.xiaomi.infra.galaxy.lcs.metric.lib.utils.FalconWriter;
import com.xiaomi.infra.galaxy.rpc.thrift.Credential;
import com.xiaomi.infra.galaxy.talos.admin.TalosAdmin;
import com.xiaomi.infra.galaxy.talos.client.Constants;
import com.xiaomi.infra.galaxy.talos.client.NamedThreadFactory;
import com.xiaomi.infra.galaxy.talos.client.ScheduleInfoCache;
import com.xiaomi.infra.galaxy.talos.client.TalosClientFactory;
import com.xiaomi.infra.galaxy.talos.client.TopicAbnormalCallback;
import com.xiaomi.infra.galaxy.talos.client.Utils;
import com.xiaomi.infra.galaxy.talos.thrift.ConsumerService;
import com.xiaomi.infra.galaxy.talos.thrift.GetDescribeInfoRequest;
import com.xiaomi.infra.galaxy.talos.thrift.GetDescribeInfoResponse;
import com.xiaomi.infra.galaxy.talos.thrift.GetTopicOffsetRequest;
import com.xiaomi.infra.galaxy.talos.thrift.OffsetInfo;
import com.xiaomi.infra.galaxy.talos.thrift.TopicTalosResourceName;

public class GreedyConsumer {
	private enum GREEDYCONSUMER_STATE {
		ACTIVE,
		DISABLED,
		SHUTDOWN,
	}

	/**
	 * Check Partition Number Task
	 * if partition number change, create new greedy fetcher
	 */
	private class CheckPartitionNumTask implements Runnable {
		@Override
		public void run() {
			GetDescribeInfoResponse response;
			try {
				response = talosAdmin.getDescribeInfo(new GetDescribeInfoRequest(topicName));
			} catch (Throwable throwable) {
				LOG.error("Exception in CheckPartitionNumTask:", throwable);
				//if throwable instance of TopicNotExist, disable greedy consumer
				if (Utils.isTopicNotExist(throwable)) {
					disableGreedyConsumer(throwable);
				}
				return;
			}

			if (!topicTalosResourceName.equals(response.getTopicTalosResourceName())) {
				String errMsg = "The topic:" + topicTalosResourceName.getTopicTalosResourceName() +
						"not exist. It might have been deleted. The greedy consumer will be disabled";
				LOG.error(errMsg);
				disableGreedyConsumer(new Throwable(errMsg));
				return;
			}

			int topicPartitionNum = response.getPartitionNumber();
			if (partitionNumber < topicPartitionNum) {
				//increase greedyMessageFetcher(not allow decreasing)
				adjustGreedyPartitionFetcher(topicPartitionNum);
				LOG.info("Partition number changes from " + partitionNumber + "to " + topicPartitionNum);
				setPartitionNumber(topicPartitionNum);
			}
		}
	}

	/**
	 * Checkpoint Task
	 * when excute this task,commit whole topic offsets to local file
	 */
	private class CheckpointTask implements Runnable {

		@Override
		public void run() {
			Map<Integer, Long> curCheckPoint = getCurCheckPoint();
			if (!shouldFlush(curCheckPoint)) {
				return;
			}
			String filePath = talosConsumerConfig.getLocalOffsetPath();
			String tmpfileName = Utils.getTempFileNameForTopic(filePath, workerId, topicName);
			String localFileName = Utils.getFileNameForTopic(filePath, workerId, topicName);
			File topicOffsetFile = new File(tmpfileName);
			FileOutputStream ps = null;

			try {
				if (!topicOffsetFile.getParentFile().exists()) {
					topicOffsetFile.getParentFile().mkdirs();
				}
				if (!topicOffsetFile.exists()) {
					topicOffsetFile.createNewFile();
				}
				ps = new FileOutputStream(topicOffsetFile);
				OutputStreamWriter writer = new OutputStreamWriter(ps);
				for (Map.Entry<Integer, Long> entry : curCheckPoint.entrySet()) {
					writer.append(entry.getKey() + Constants.TALOS_IDENTIFIER_DELIMITER
							+ entry.getValue() + "\n");
				}
				writer.close();
				File localFile = new File(localFileName);
				topicOffsetFile.renameTo(localFile);
				partitionCheckPointFromFile = curCheckPoint;
			} catch (IOException e) {
				LOG.error("Commit offset to file for topic:" + topicName + " failed!", e);
			} finally {
				if (ps != null) {
					try {
						ps.close();
					} catch (IOException e) {
						LOG.error("OutputStream close IOException for topic:" + topicName + "failed!", e);
					}
				}
			}

		}
	}

	private class GreedyConsumerMonitorTask implements Runnable {
		@Override
		public void run() {
			try {
				pushMetricData();
			} catch (Exception e) {
				LOG.error("Push metric data to falcon failed.", e);
			}
		}
	}

	private AtomicReference<GREEDYCONSUMER_STATE> greedyConsumerState;
	private static final Logger LOG = LoggerFactory.getLogger(GreedyConsumer.class);
	private TalosConsumerConfig talosConsumerConfig;
	private GreedyMessageReaderFactory messageReaderFactory;
	private MessageProcessorFactory messageProcessorFactory;
	private TalosClientFactory talosClientFactory;
	private ScheduleInfoCache scheduleInfoCache;
	private TalosAdmin talosAdmin;
	private ConsumerService.Iface consumerClient;
	private TopicAbnormalCallback topicAbnormalCallback;
	private Map<Integer, Long> outerCheckPoint;
	private Map<Integer, Long> partitionCheckPointFromFile;
	private ReadWriteLock readWriteLock;
	private String workerId;
	private Map<Integer, GreedyPartitionFetcher> partitionFetcherMap =
			new ConcurrentHashMap<Integer, GreedyPartitionFetcher>();

	// init by getting from rpc call as follows
	private String topicName;
	private int partitionNumber;
	private TopicTalosResourceName topicTalosResourceName;

	private ScheduledExecutorService partitionScheduleExecutor;
	private ScheduledExecutorService checkpointScheduleExecutor;
	private Future partitionCheckFuture;
	private Future checkpointFuture;

	private FalconWriter falconWriter;
	private ScheduledExecutorService greedyMonitorThread;

	public GreedyConsumer(TalosConsumerConfig consumerConfig, Credential credential,
			String topicName, GreedyMessageReaderFactory messageReaderFactory,
			MessageProcessorFactory messageProcessorFactory, TopicAbnormalCallback abnormalCallback,
			String workerId, Map<Integer, Long> outerCheckPoint) throws TException {
		greedyConsumerState = new AtomicReference<GREEDYCONSUMER_STATE>(GREEDYCONSUMER_STATE.ACTIVE);
		talosConsumerConfig = consumerConfig;
		talosClientFactory = new TalosClientFactory(talosConsumerConfig, credential);
		this.messageReaderFactory = messageReaderFactory;
		this.messageProcessorFactory = messageProcessorFactory;
		this.topicName = topicName;
		this.workerId = workerId;
		talosAdmin = new TalosAdmin(talosClientFactory);
		readWriteLock = new ReentrantReadWriteLock();
		this.topicTalosResourceName = talosAdmin.getDescribeInfo(
				new GetDescribeInfoRequest(topicName)).getTopicTalosResourceName();
		consumerClient = talosClientFactory.newConsumerClient();
		topicAbnormalCallback = abnormalCallback;
		this.outerCheckPoint = outerCheckPoint == null ? new HashMap<Integer, Long>() : outerCheckPoint;
		partitionCheckPointFromFile = initCheckPointFromFile();
		this.scheduleInfoCache = ScheduleInfoCache.getScheduleInfoCache(this.topicTalosResourceName,
				consumerConfig, talosClientFactory.newMessageClient(), talosClientFactory);
		this.falconWriter = FalconWriter.getFalconWriter(
				consumerConfig.getFalconUrl(), new Slf4jLogger(LOG));

		partitionScheduleExecutor = Executors.newSingleThreadScheduledExecutor(
				new NamedThreadFactory("greedy-consumer-partitionCheck-" + topicName));
		checkpointScheduleExecutor = Executors.newSingleThreadScheduledExecutor(
				new NamedThreadFactory("greedy-consumer-checkpoint-" + topicName));
		greedyMonitorThread = Executors.newSingleThreadScheduledExecutor(
				new NamedThreadFactory("greedy-consumer-monitor" + topicName));

		// check and get topic info such as partitionNumber
		checkAndGetTopicInfo(topicTalosResourceName);

		//start partitionFetcherTask/CheckPartitionTask/CheckpointTask
		initGreedyPartitionFetcher();
		initCheckPartitionTask();
		initCheckpointTask();
		initGreedyMonitorTask();

		LOG.info("Init a greedyConsumer for topic:" + topicTalosResourceName.getTopicTalosResourceName() + ", " +
				"partitions:" + partitionNumber);
	}

	// greedyConsumer constructor without outerCheckpoint
	public GreedyConsumer(TalosConsumerConfig consumerConfig, Credential credential,
			String topicName, GreedyMessageReaderFactory messageReaderFactory,
			MessageProcessorFactory messageProcessorFactory, TopicAbnormalCallback abnormalCallback,
			String workerId) throws TException {
		this(consumerConfig, credential, topicName, messageReaderFactory, messageProcessorFactory,
				abnormalCallback, workerId, new HashMap<Integer, Long>());
	}

	//for test
	public GreedyConsumer(TalosConsumerConfig consumerConfig, TopicTalosResourceName talosResourceName,
			String topicName, String workerId, TopicAbnormalCallback abnormalCallback, ConsumerService.Iface consumerClientMock,
			TalosAdmin talosAdminMock, Map<Integer, GreedyPartitionFetcher> fetcherMap) throws Exception {
		greedyConsumerState = new AtomicReference<GREEDYCONSUMER_STATE>(GREEDYCONSUMER_STATE.ACTIVE);
		this.workerId = workerId;
		this.topicName = topicName;
		partitionFetcherMap = fetcherMap;
		talosConsumerConfig = consumerConfig;
		topicAbnormalCallback = abnormalCallback;
		consumerClient = consumerClientMock;
		talosAdmin = talosAdminMock;
		readWriteLock = new ReentrantReadWriteLock();
		partitionCheckPointFromFile = initCheckPointFromFile();
		this.scheduleInfoCache = ScheduleInfoCache.getScheduleInfoCache(talosResourceName,
				consumerConfig, null, null);

		partitionScheduleExecutor = Executors.newSingleThreadScheduledExecutor(
				new NamedThreadFactory("talos-consumer-partitionCheck-" + topicName));
		checkpointScheduleExecutor = Executors.newSingleThreadScheduledExecutor(
				new NamedThreadFactory("greedy-consumer-checkpoint-" + topicName));

		LOG.info("The worker: " + workerId + "is initializing");
		// check and get topic info such as partitionNumber
		checkAndGetTopicInfo(talosResourceName);
		//start partitionFetcherTask/CheckPartitionTask/CheckpointTask
		//initGreedyPartitionFetcher();
		initCheckPartitionTask();
		initCheckpointTask();
	}

	public Map<Integer, Long> initCheckPointFromFile() {
		String filePath = talosConsumerConfig.getLocalOffsetPath();
		String fileName = Utils.getFileNameForTopic(filePath, workerId, topicName);
		Map<Integer, Long> checkpointFromFile = new HashMap<Integer, Long>();
		try {
			checkpointFromFile = Utils.getOffsetMapFromFile(fileName);
		} catch (IOException e) {
			LOG.warn("Init checkpoint from file: " + fileName + "failed!", e);
		}
		return checkpointFromFile;
	}

	private void checkAndGetTopicInfo(TopicTalosResourceName topicTalosResourceName)
			throws TException {
		topicName = Utils.getTopicNameByResourceName(topicTalosResourceName.getTopicTalosResourceName());
		GetDescribeInfoResponse response = talosAdmin.getDescribeInfo(new GetDescribeInfoRequest(topicName));

		if (!topicTalosResourceName.equals(response.getTopicTalosResourceName())) {
			LOG.info("The consumer initialize failed by topic not found");
			throw new IllegalArgumentException("The topic: " + topicTalosResourceName.getTopicTalosResourceName()
					+ " not found");
		}
		setPartitionNumber(response.getPartitionNumber());
		this.topicTalosResourceName = topicTalosResourceName;
		LOG.info("The worker: " + workerId + " check and get topic info done");
	}

	private synchronized void initGreedyPartitionFetcher() {
		for (int partitionId = 0; partitionId < partitionNumber; ++partitionId) {
			createPartitionFetcher(partitionId);
		}
	}

	private synchronized void initCheckPartitionTask() {
		//check and update partition number every 3 minutes by default
		partitionCheckFuture = partitionScheduleExecutor.scheduleAtFixedRate(new CheckPartitionNumTask(),
				talosConsumerConfig.getPartitionCheckInterval(), talosConsumerConfig.getPartitionCheckInterval(),
				TimeUnit.MILLISECONDS);
	}

	private synchronized void createPartitionFetcher(int partitionId) {
		GreedyPartitionFetcher greedyPartitionFetcher = new GreedyPartitionFetcher(topicName, topicTalosResourceName,
				partitionId, talosConsumerConfig, workerId, consumerClient, talosClientFactory.newMessageClient(),
				messageProcessorFactory.createProcessor(), messageReaderFactory.createMessageReader(talosConsumerConfig),
				outerCheckPoint.get(partitionId));
		partitionFetcherMap.put(partitionId, greedyPartitionFetcher);
	}

	private void setPartitionNumber(int partitionNum) {
		readWriteLock.writeLock().lock();
		partitionNumber = partitionNum;
		readWriteLock.writeLock().unlock();
	}

	public synchronized void initCheckpointTask() {
		//do checkpoint task every minute by default
		checkpointFuture = checkpointScheduleExecutor.scheduleAtFixedRate(new CheckpointTask(),
				talosConsumerConfig.getCheckpointInterval(), talosConsumerConfig.getCheckpointInterval(),
				TimeUnit.MILLISECONDS);
	}

	// get current committed offset of every serving partition
	public Map<Integer, Long> getCurCheckPoint() {
		Map<Integer, Long> curCheckPoint = new HashMap<Integer, Long>();
		readWriteLock.readLock().lock();
		for (Map.Entry<Integer, GreedyPartitionFetcher> entry : partitionFetcherMap.entrySet()) {
			curCheckPoint.put(entry.getKey(), entry.getValue().getCurCheckPoint());
		}
		readWriteLock.readLock().unlock();
		return curCheckPoint;
	}

	// flush to flie when new offset occurs or partition numbers increased
	public boolean shouldFlush(Map<Integer, Long> curCheckpoint) {
		if (curCheckpoint.size() > partitionCheckPointFromFile.size()) {
			return true;
		}
		if (curCheckpoint != partitionCheckPointFromFile) {
			for (Integer paritionId : partitionCheckPointFromFile.keySet()) {
				if (partitionCheckPointFromFile.get(paritionId) < curCheckpoint.get(paritionId)) {
					return true;
				}
			}
		}

		return false;
	}

	public boolean isActive() {
		return greedyConsumerState.get() == GREEDYCONSUMER_STATE.ACTIVE;
	}

	public boolean isDisabled() {
		return greedyConsumerState.get() == GREEDYCONSUMER_STATE.DISABLED;
	}

	public boolean isShutdowned() {
		return greedyConsumerState.get() == GREEDYCONSUMER_STATE.SHUTDOWN;
	}

	private synchronized void disableGreedyConsumer(Throwable throwable) {
		if (!isActive()) {
			return;
		}
		greedyConsumerState.set(GREEDYCONSUMER_STATE.DISABLED);
		shutDown();
		topicAbnormalCallback.abnormalHandler(topicTalosResourceName, throwable);
	}

	private void shutDownAllFetcher() {
		for (Map.Entry<Integer, GreedyPartitionFetcher> entry : partitionFetcherMap.entrySet()) {
			entry.getValue().shutDown();
		}
	}

	private void adjustGreedyPartitionFetcher(int newPartitionNum) {
		for (int partitionId = partitionNumber; partitionId < newPartitionNum; ++partitionId) {
			createPartitionFetcher(partitionId);
		}
		LOG.info("Adjust greedyPartitionFetcher and partitionNumber from:" +
				partitionNumber + "to:" + newPartitionNum);
	}

	private void initGreedyMonitorTask() {
		if (talosConsumerConfig.isOpenClientMonitor()) {
			//push metric data to falcon every minute
			greedyMonitorThread.scheduleAtFixedRate(new GreedyConsumerMonitorTask(),
					talosConsumerConfig.getReportMetricIntervalMillis(),
					talosConsumerConfig.getReportMetricIntervalMillis(), TimeUnit.MILLISECONDS);
		}
	}

	private void pushMetricData() {
		JsonArray jsonArray = new JsonArray();
		for (Map.Entry<Integer, GreedyPartitionFetcher> entry : partitionFetcherMap.entrySet()) {
			jsonArray.addAll(entry.getValue().getFalconData());
		}
		jsonArray.addAll(getPartitionLag());
		falconWriter.pushFaclonData(jsonArray.toString());
	}

	public JsonArray getPartitionLag() {
		JsonArray jsonArray = new JsonArray();
		GetTopicOffsetRequest request = new GetTopicOffsetRequest(topicTalosResourceName);
		try {
			List<OffsetInfo> offsetInfoList = talosAdmin.getTopicOffset(request);
			Map<Integer, Long> curCheckPoint = getCurCheckPoint();
			for (OffsetInfo offsetInfo : offsetInfoList) {
				long endOffset = offsetInfo.getEndOffset();
				long curCommitOffset = curCheckPoint.get(offsetInfo.getPartitionId());
				long lag = endOffset > curCommitOffset ? endOffset - curCommitOffset : 0;
				JsonObject jsonObject = getBasicData(String.valueOf(offsetInfo.getPartitionId()));
				jsonObject.addProperty("metric", Constants.CONSUMER_OFFSET_LAG);
				jsonObject.addProperty("value", lag);
				jsonArray.add(jsonObject);
			}
		} catch (TException e) {
			LOG.error("Get endOffset failed!", e);
		}

		return jsonArray;
	}

	private JsonObject getBasicData(String partitionId) {
		String tag = "clusterName=" + talosConsumerConfig.getClusterName();
		tag += ",topicName=" + topicName;
		tag += ",partitionId=" + partitionId;
		tag += ",ip=" + talosConsumerConfig.getClientIp();
		tag += ",type=" + talosConsumerConfig.getAlertType();

		JsonObject basicData = new JsonObject();
		basicData.addProperty("endpoint", talosConsumerConfig.getGreedyMetricFalconEndpoint()
				+ workerId);
		basicData.addProperty("timestamp", System.currentTimeMillis() / 1000);
		basicData.addProperty("step", talosConsumerConfig.getMetricFalconStep() / 1000);
		basicData.addProperty("counterType", "GAUGE");
		basicData.addProperty("tags", tag);
		return basicData;
	}

	public void shutDown() {
		LOG.info("Worker: " + workerId + " is shutting down...");
		greedyConsumerState.set(GREEDYCONSUMER_STATE.SHUTDOWN);
		shutDownAllFetcher();
		partitionCheckFuture.cancel(false);
		checkpointFuture.cancel(false);
		partitionScheduleExecutor.shutdownNow();
		checkpointScheduleExecutor.shutdownNow();
		greedyMonitorThread.shutdownNow();
		scheduleInfoCache.shutDown(topicTalosResourceName);
		LOG.info("Worker: " + workerId + " shutdown ");
	}
}
