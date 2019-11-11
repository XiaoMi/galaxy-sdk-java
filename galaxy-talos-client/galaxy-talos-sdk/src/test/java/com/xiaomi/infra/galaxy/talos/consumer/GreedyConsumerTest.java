/**
 * Copyright 2019, Xiaomi.
 * All rights reserved.
 * Author: huyumei@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.consumer;

import java.io.File;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

//import org.graalvm.compiler.api.replacements.Snippet;
import org.junit.Test;
import org.junit.Before;
import org.junit.After;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.infra.galaxy.talos.admin.TalosAdmin;
import com.xiaomi.infra.galaxy.talos.client.SimpleTopicAbnormalCallback;
import com.xiaomi.infra.galaxy.talos.client.TalosClientConfigKeys;
import com.xiaomi.infra.galaxy.talos.client.Utils;
import com.xiaomi.infra.galaxy.talos.thrift.ConsumerService;
import com.xiaomi.infra.galaxy.talos.thrift.ErrorCode;
import com.xiaomi.infra.galaxy.talos.thrift.GalaxyTalosException;
import com.xiaomi.infra.galaxy.talos.thrift.GetDescribeInfoRequest;
import com.xiaomi.infra.galaxy.talos.thrift.GetDescribeInfoResponse;
import com.xiaomi.infra.galaxy.talos.thrift.Topic;
import com.xiaomi.infra.galaxy.talos.thrift.TopicAttribute;
import com.xiaomi.infra.galaxy.talos.thrift.TopicInfo;
import com.xiaomi.infra.galaxy.talos.thrift.TopicState;
import com.xiaomi.infra.galaxy.talos.thrift.TopicTalosResourceName;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

public class GreedyConsumerTest {
	private static final Logger LOG = LoggerFactory.getLogger(PartitionFetcherTest.class);
	private static final String topicName = "MyTopic";
	private static final String ownerId = "12345";
	private static final int partitionNum = 5;
	private static final int partitionNum2 = 6;
	private static final String resourceName = "12345#MyTopic#34595fkdiso456i390";
	private static final String resourceName2 = "12345#MyTopic#34595fkdiso456i23";
	private static final String consumerGroup = "MyConsumerGroup";
	private static final String clientIdPrefix = "TestClient1";
	private static final String workerId = "worker1";

	private static final TopicTalosResourceName talosResourceName =
			new TopicTalosResourceName(resourceName);
	private static final TopicTalosResourceName talosResourceName2 =
			new TopicTalosResourceName(resourceName2);
	private static final TopicInfo topicInfo = new TopicInfo(
			topicName, talosResourceName, ownerId);
	private static final TopicInfo topicInfo2 = new TopicInfo(
			topicName, talosResourceName2, ownerId);
	private static final TopicAttribute topicAttribute =
			new TopicAttribute().setPartitionNumber(partitionNum);
	private static final TopicAttribute topicAttribute2 =
			new TopicAttribute().setPartitionNumber(partitionNum2);
	private static final Topic topic = new Topic(topicInfo, topicAttribute,
			new TopicState());
	// resourceName changed
	private static final Topic topic2 = new Topic(topicInfo2, topicAttribute,
			new TopicState());
	// partitionNumber changed
	private static final Topic topic3 = new Topic(topicInfo, topicAttribute2,
			new TopicState());

	private static final Map<Integer, GreedyPartitionFetcher> fetcherMap =
			new HashMap<Integer, GreedyPartitionFetcher>();
	private static final GreedyPartitionFetcher partitionFetcher0 = Mockito.mock(
			GreedyPartitionFetcher.class);
	private static final GreedyPartitionFetcher partitionFetcher1 = Mockito.mock(
			GreedyPartitionFetcher.class);
	private static final GreedyPartitionFetcher partitionFetcher2 = Mockito.mock(
			GreedyPartitionFetcher.class);
	private static final GreedyPartitionFetcher partitionFetcher3 = Mockito.mock(
			GreedyPartitionFetcher.class);
	private static final GreedyPartitionFetcher partitionFetcher4 = Mockito.mock(
			GreedyPartitionFetcher.class);
	private static final GreedyPartitionFetcher partitionFetcher5 = Mockito.mock(
			GreedyPartitionFetcher.class);

	private static TalosConsumerConfig consumerConfig;
	private static TalosAdmin talosAdminMock;
	private static ConsumerService.Iface consumerClientMock;

	@Before
	public void setUp() throws Exception {
		Properties properties = new Properties();
		// check partition number interval 200 ms
		properties.setProperty(
				TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_CHECK_PARTITION_INTERVAL, "200");
		//auto commit offset,true
		properties.setProperty(
				TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_CHECKPOINT_AUTO_COMMIT, "true");
		properties.setProperty(
				TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_FETCH_INTERVAL, "0");
		properties.setProperty(
				TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_COMMIT_OFFSET_INTERVAL, "0");
		properties.setProperty(
				TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_COMMIT_OFFSET_THRESHOLD, "0");
		properties.setProperty(
				TalosClientConfigKeys.GALAXY_TALOS_GREEDYCONSUMER_SYNCHRONOUS_CHECKPOINT,"false");
		properties.setProperty(
				TalosClientConfigKeys.GALAXY_TALOS_SERVICE_ENDPOINT, "testURI");
		//greedyConsumer checkpoint interval
		properties.setProperty(
				TalosClientConfigKeys.GALAXY_TALOS_GREEDYCONSUMER_CHECKPOINT_INTERVAL,"1");
		//local file path
		properties.setProperty(
				TalosClientConfigKeys.GALAXY_TALOS_GREEDYCONSUMER_LOCAL_OFFSET_PATH, "/.talos/offset/");

		// do not check parameter validity for unit test
		consumerConfig = new TalosConsumerConfig(properties, false);
		talosAdminMock = Mockito.mock(TalosAdmin.class);
		consumerClientMock = Mockito.mock(ConsumerService.Iface.class);

		fetcherMap.put(0, partitionFetcher0);
		fetcherMap.put(1, partitionFetcher1);
		fetcherMap.put(2, partitionFetcher2);
		fetcherMap.put(3, partitionFetcher3);
		fetcherMap.put(4, partitionFetcher4);
		fetcherMap.put(5, partitionFetcher5);

		// construct a getDescribeInfoResponse
		GetDescribeInfoResponse getDescribeInfoResponse = new GetDescribeInfoResponse(
				talosResourceName, partitionNum);

		when(talosAdminMock.getDescribeInfo(any(GetDescribeInfoRequest.class))).thenReturn(
				getDescribeInfoResponse);

	}

	@After
	public void tearDown() throws Exception {
	}

	//1) checkAndGetTopicInfoFailed
	@Test (expected = IllegalArgumentException.class)
	public void testCheckAndGetTopicInfoFailed() throws Exception {
		LOG.info("testCheckAndGetTopicInfoFailed start");
		//get describe info
		GetDescribeInfoResponse getDescribeInfoResponse = new GetDescribeInfoResponse(
				talosResourceName2,partitionNum);
		when(talosAdminMock.getDescribeInfo(any(GetDescribeInfoRequest.class))).thenReturn(
				getDescribeInfoResponse);

		GreedyConsumer greedyConsumer = new GreedyConsumer(consumerConfig, talosResourceName,
				topicName, workerId, new SimpleTopicAbnormalCallback(), consumerClientMock,
				talosAdminMock, fetcherMap);

		LOG.info("testCheckAndGetTopicInfoFailed done");
	}

	//2)test CheckpointTask
	@Test
	public void testCheckpointTask()throws Exception {
		LOG.info("[testCheckpoint] start");
		when(partitionFetcher0.getCurCheckPoint()).thenReturn(new Long(100));
		when(partitionFetcher1.getCurCheckPoint()).thenReturn(new Long(200));
		when(partitionFetcher2.getCurCheckPoint()).thenReturn(new Long(300));
		when(partitionFetcher3.getCurCheckPoint()).thenReturn(new Long(400));
		when(partitionFetcher4.getCurCheckPoint()).thenReturn(new Long(500));
		when(partitionFetcher5.getCurCheckPoint()).thenReturn(new Long(600));

		GreedyConsumer greedyConsumer = new GreedyConsumer(consumerConfig, talosResourceName,
				topicName, workerId, new SimpleTopicAbnormalCallback(), consumerClientMock,
				talosAdminMock,fetcherMap);
		Map<Integer, Long> curCheckpoint = greedyConsumer.getCurCheckPoint();
		assertEquals(true, greedyConsumer.shouldFlush(curCheckpoint));
		Thread.sleep(10050);
		Map<Integer, Long> checkpointFromFile = greedyConsumer.initCheckPointFromFile();
		curCheckpoint = greedyConsumer.getCurCheckPoint();
		assertEquals(checkpointFromFile, curCheckpoint );
		assertEquals(false, greedyConsumer.shouldFlush(curCheckpoint));

		greedyConsumer.shutDown();
		LOG.info("[testCheckpoint] done");
	}

	//3) describeTopic not exist error
	@Test
	public void testDescribeTopicNotExist() throws Exception {
		LOG.info("[testDescribeTopicNotExist] start");
		GetDescribeInfoResponse getDescribeInfoResponse = new GetDescribeInfoResponse(
				talosResourceName,partitionNum);
		doReturn(getDescribeInfoResponse).doThrow(new GalaxyTalosException().setErrorCode(
				ErrorCode.TOPIC_NOT_EXIST)).when(talosAdminMock).getDescribeInfo(
						any(GetDescribeInfoRequest.class));

		GreedyConsumer greedyConsumer = new GreedyConsumer(consumerConfig, talosResourceName,
				topicName, workerId, new SimpleTopicAbnormalCallback(), consumerClientMock,
				talosAdminMock,fetcherMap);

		greedyConsumer.shutDown();
		LOG.info("testDescribeTopicNotExist done");
	}

	//4)describeTopic resourceName changed, old topic deleted
	@Test (expected = IllegalArgumentException.class)
	public void testDescribeTopicResourceNameChanged() throws Exception {
		LOG.info("[testDescribeTopicResourceNameChanged] start");
		GetDescribeInfoResponse getDescribeInfoResponse = new GetDescribeInfoResponse(
				talosResourceName, partitionNum);
		GetDescribeInfoResponse getDescribeInfoResponse2 = new GetDescribeInfoResponse(
				talosResourceName2, partitionNum);
		when(talosAdminMock.getDescribeInfo(any(GetDescribeInfoRequest.class))).thenReturn(
				getDescribeInfoResponse2).thenReturn(getDescribeInfoResponse2);

		GreedyConsumer greedyConsumer = new GreedyConsumer(consumerConfig, talosResourceName,
				topicName, workerId, new SimpleTopicAbnormalCallback(),consumerClientMock,
				talosAdminMock, fetcherMap);
		greedyConsumer.shutDown();
		LOG.info("[testDescribeTopicResourceNameChanged] done");
	}

	//5) test create local offset file success
	@Test
	public void testCreateLocalOffsetFile() throws Exception {
		LOG.info("[testCreateLocalOffsetFile] start");
		String fileName = Utils.getFileNameForTopic(consumerConfig.getLocalOffsetPath(), workerId,
				topicName);
		File fileForTopic = new File(fileName);
		assertEquals(false, fileForTopic.exists());
		GreedyConsumer greedyConsumer = new GreedyConsumer(consumerConfig, talosResourceName,
				topicName, workerId, new SimpleTopicAbnormalCallback(), consumerClientMock,
				talosAdminMock,fetcherMap);
		Thread.sleep(10050);
		File newFileForTopic = new File(fileName);
		assertEquals(true, newFileForTopic.exists());
		greedyConsumer.shutDown();
		LOG.info("[testCreateLocalOffsetFile] done");
	}

} 
