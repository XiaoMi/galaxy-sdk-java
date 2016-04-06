/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: yongxing@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.consumer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.infra.galaxy.talos.admin.TalosAdmin;
import com.xiaomi.infra.galaxy.talos.client.SimpleTopicAbnormalCallback;
import com.xiaomi.infra.galaxy.talos.client.TalosClientConfigKeys;
import com.xiaomi.infra.galaxy.talos.client.Utils;
import com.xiaomi.infra.galaxy.talos.thrift.ConsumerService;
import com.xiaomi.infra.galaxy.talos.thrift.DescribeTopicRequest;
import com.xiaomi.infra.galaxy.talos.thrift.ErrorCode;
import com.xiaomi.infra.galaxy.talos.thrift.GalaxyTalosException;
import com.xiaomi.infra.galaxy.talos.thrift.LockWorkerRequest;
import com.xiaomi.infra.galaxy.talos.thrift.LockWorkerResponse;
import com.xiaomi.infra.galaxy.talos.thrift.QueryWorkerRequest;
import com.xiaomi.infra.galaxy.talos.thrift.QueryWorkerResponse;
import com.xiaomi.infra.galaxy.talos.thrift.RenewRequest;
import com.xiaomi.infra.galaxy.talos.thrift.RenewResponse;
import com.xiaomi.infra.galaxy.talos.thrift.Topic;
import com.xiaomi.infra.galaxy.talos.thrift.TopicAttribute;
import com.xiaomi.infra.galaxy.talos.thrift.TopicInfo;
import com.xiaomi.infra.galaxy.talos.thrift.TopicState;
import com.xiaomi.infra.galaxy.talos.thrift.TopicTalosResourceName;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

public class TalosConsumerTest {
  private static final Logger LOG = LoggerFactory.getLogger(PartitionFetcherTest.class);
  private static final String topicName = "MyTopic";
  private static final String ownerId = "12345";
  private static final int partitionNum = 5;
  private static final int partitionNum2 = 6;
  private static final String resourceName = "12345#MyTopic#34595fkdiso456i390";
  private static final String resourceName2 = "12345#MyTopic#34595fkdiso456i23";
  private static final String consumerGroup = "MyConsumerGroup";
  private static final String clientIdPrefix = "TestClient1";
  private static final String clientIdPrefix2 = "TestClient2";
  private static final String clientIdPrefix3 = "TestClient3";
  private static final String workerId = Utils.generateClientId(clientIdPrefix);
  private static final String workerId2 = Utils.generateClientId(clientIdPrefix2);
  private static final String workerId3 = Utils.generateClientId(clientIdPrefix3);

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

  private static final Map<Integer, PartitionFetcher> fetcherMap =
      new HashMap<Integer, PartitionFetcher>();
  private static final PartitionFetcher partitionFetcher0 = Mockito.mock(
      PartitionFetcher.class);
  private static final PartitionFetcher partitionFetcher1 = Mockito.mock(
      PartitionFetcher.class);
  private static final PartitionFetcher partitionFetcher2 = Mockito.mock(
      PartitionFetcher.class);
  private static final PartitionFetcher partitionFetcher3 = Mockito.mock(
      PartitionFetcher.class);
  private static final PartitionFetcher partitionFetcher4 = Mockito.mock(
      PartitionFetcher.class);
  private static final PartitionFetcher partitionFetcher5 = Mockito.mock(
      PartitionFetcher.class);

  private static TalosConsumerConfig consumerConfig;
  private static TalosAdmin talosAdminMock;
  private static ConsumerService.Iface consumerClientMock;

  @Before
  public void setUp() throws Exception {
    Configuration configuration = new Configuration();
    // check partition number interval 200 ms
    configuration.setInt(
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_CHECK_PARTITION_INTERVAL, 200);
    // check worker info interval 300 ms
    configuration.setInt(
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_CHECK_WORKER_INFO_INTERVAL, 300);
    // renew interval 3050 ms
    configuration.setInt(
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_RENEW_INTERVAL, 300);
    configuration.setInt(
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_RENEW_MAX_RETRY, 0);
    configuration.setInt(
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_REGISTER_MAX_RETRY, 0);

    // do not check parameter validity for unit test
    consumerConfig = new TalosConsumerConfig(configuration, false);
    talosAdminMock = Mockito.mock(TalosAdmin.class);
    consumerClientMock = Mockito.mock(ConsumerService.Iface.class);

    fetcherMap.put(0, partitionFetcher0);
    fetcherMap.put(1, partitionFetcher1);
    fetcherMap.put(2, partitionFetcher2);
    fetcherMap.put(3, partitionFetcher3);
    fetcherMap.put(4, partitionFetcher4);
    fetcherMap.put(5, partitionFetcher5);
  }

  @After
  public void tearDown() {
  }

  // 1) checkAndGetTopicInfoFailed
  @Test (expected = IllegalArgumentException.class)
  public void testCheckAndGetTopicInfoFailed() throws Exception {
    // partition check
    when(talosAdminMock.describeTopic(any(DescribeTopicRequest.class)))
        .thenReturn(topic2);

    TalosConsumer consumer = new TalosConsumer(consumerGroup, consumerConfig,
        talosResourceName, workerId, new SimpleTopicAbnormalCallback(),
        consumerClientMock, talosAdminMock, fetcherMap);
  }

  // 2) registerSelf failed, init failed
  @Test (expected = RuntimeException.class)
  public void testRegisterSelfFailed() throws Exception {
    // partition check
    when(talosAdminMock.describeTopic(any(DescribeTopicRequest.class)))
        .thenReturn(topic);
    // register self
    LockWorkerResponse lockWorkerResponse = new LockWorkerResponse(false);
    when(consumerClientMock.lockWorker(any(LockWorkerRequest.class)))
        .thenReturn(lockWorkerResponse);

    TalosConsumer consumer = new TalosConsumer(consumerGroup, consumerConfig,
        talosResourceName, workerId, new SimpleTopicAbnormalCallback(),
        consumerClientMock, talosAdminMock, fetcherMap);
  }

  // 3) lock -> balance
  @Test
  public void testLockBalance() throws Exception {
    LOG.info("[testLockBalance] start");
    // partition check
    when(talosAdminMock.describeTopic(any(DescribeTopicRequest.class)))
        .thenReturn(topic);
    // register self
    LockWorkerResponse lockWorkerResponse = new LockWorkerResponse(true);
    when(consumerClientMock.lockWorker(any(LockWorkerRequest.class)))
        .thenReturn(lockWorkerResponse);
    // renew check
    when(consumerClientMock.renew(any(RenewRequest.class)))
        .thenReturn(new RenewResponse(true, new ArrayList<Integer>()));
    // worker info check
    Map<String, List<Integer>> workerInfo = new HashMap<String, List<Integer>>();
    List<Integer> partitionList = new ArrayList<Integer>();
    partitionList.add(3);
    partitionList.add(0);
    workerInfo.put(workerId, new ArrayList<Integer>());
    workerInfo.put(workerId2, partitionList);
    Map<String, List<Integer>> workerInfo2 = new HashMap<String, List<Integer>>();
    List<Integer> partitionList2 = new ArrayList<Integer>();
    partitionList2.add(1);
    partitionList2.add(4);
    workerInfo2.put(workerId, partitionList2);
    workerInfo2.put(workerId2, partitionList);
    // first not balance, then balance for serving partition number
    when(consumerClientMock.queryWorker(any(QueryWorkerRequest.class)))
        .thenReturn(new QueryWorkerResponse(workerInfo))
        .thenReturn(new QueryWorkerResponse(workerInfo2));
    // for renew/steal partition/re-balance
    when(partitionFetcher1.isServing()).thenReturn(false).thenReturn(true);
    when(partitionFetcher4.isServing()).thenReturn(false).thenReturn(true);
    when(partitionFetcher1.isHoldingLock()).thenReturn(false).thenReturn(true);
    when(partitionFetcher4.isHoldingLock()).thenReturn(false).thenReturn(true);

    TalosConsumer consumer = new TalosConsumer(consumerGroup, consumerConfig,
        talosResourceName, workerId, new SimpleTopicAbnormalCallback(),
        consumerClientMock, talosAdminMock, fetcherMap);
    // sleep for 2 times balance
    Thread.sleep(800);
    consumer.shutDown();
    LOG.info("[testLockBalance] done");
  }


  // 4) worker online: lock -> unlock -> balance
  @Test
  public void testWorkerOnlineLockUnlock() throws Exception {
    LOG.info("[testWorkerOnlineLockUnlock] start");
    // partition check
    when(talosAdminMock.describeTopic(any(DescribeTopicRequest.class)))
        .thenReturn(topic);
    // register self
    LockWorkerResponse lockWorkerResponse = new LockWorkerResponse(true);
    when(consumerClientMock.lockWorker(any(LockWorkerRequest.class)))
        .thenReturn(lockWorkerResponse);
    // renew check
    when(consumerClientMock.renew(any(RenewRequest.class)))
        .thenReturn(new RenewResponse(true, new ArrayList<Integer>()));
    // worker info check
    // first: 1 worker, 5, this worker lock 5 partitions
    Map<String, List<Integer>> workerInfo = new HashMap<String, List<Integer>>();
    workerInfo.put(workerId, new ArrayList<Integer>());
    // second: 3 worker, 2 2 1, has sort: 5 0 0, this worker unlock 3 partition
    Map<String, List<Integer>> workerInfo2 = new HashMap<String, List<Integer>>();
    List<Integer> partitionList = new ArrayList<Integer>();
    for (int i = 0; i < partitionNum; ++i) {
      partitionList.add(i);
    }
    workerInfo2.put(workerId, partitionList); // should has 5 partitions
    workerInfo2.put(workerId2, new ArrayList<Integer>());
    workerInfo2.put(workerId3, new ArrayList<Integer>());
    when(consumerClientMock.queryWorker(any(QueryWorkerRequest.class)))
        .thenReturn(new QueryWorkerResponse(workerInfo))
        .thenReturn(new QueryWorkerResponse(workerInfo2));
    // for renew/steal partition/re-balance
    // construct 3 times balance
    when(partitionFetcher0.isServing())
        .thenReturn(false).thenReturn(true).thenReturn(false);
    when(partitionFetcher1.isServing())
        .thenReturn(false).thenReturn(true).thenReturn(false);
    when(partitionFetcher2.isServing())
        .thenReturn(false).thenReturn(true).thenReturn(false);
    when(partitionFetcher3.isServing()).thenReturn(false).thenReturn(true);
    when(partitionFetcher4.isServing()).thenReturn(false).thenReturn(true);

    when(partitionFetcher0.isHoldingLock())
        .thenReturn(false).thenReturn(true).thenReturn(false);
    when(partitionFetcher1.isHoldingLock())
        .thenReturn(false).thenReturn(true).thenReturn(false);
    when(partitionFetcher2.isHoldingLock())
        .thenReturn(false).thenReturn(true).thenReturn(false);
    when(partitionFetcher3.isHoldingLock()).thenReturn(false).thenReturn(true);
    when(partitionFetcher4.isHoldingLock()).thenReturn(false).thenReturn(true);

    TalosConsumer consumer = new TalosConsumer(consumerGroup, consumerConfig,
        talosResourceName, workerId, new SimpleTopicAbnormalCallback(),
        consumerClientMock, talosAdminMock, fetcherMap);
    // sleep for 3 times balance
    Thread.sleep(1000);
    consumer.shutDown();
    LOG.info("[testWorkerOnlineLockUnlock] done");
  }


  // 5) worker offline: lock -> lock -> balance
  @Test
  public void testWorkerOfflineLockLock() throws Exception {
    LOG.info("[testWorkerOfflineLockLock] start");
    // partition check
    when(talosAdminMock.describeTopic(any(DescribeTopicRequest.class)))
        .thenReturn(topic);
    // register self
    LockWorkerResponse lockWorkerResponse = new LockWorkerResponse(true);
    when(consumerClientMock.lockWorker(any(LockWorkerRequest.class)))
        .thenReturn(lockWorkerResponse);
    // renew check
    when(consumerClientMock.renew(any(RenewRequest.class)))
        .thenReturn(new RenewResponse(true, new ArrayList<Integer>()));
    // worker info check
    // first: 2 worker, [3,2], this worker lock 2 paritions
    Map<String, List<Integer>> workerInfo = new HashMap<String, List<Integer>>();
    List<Integer> partitionList = new ArrayList<Integer>();
    partitionList.add(4);
    partitionList.add(0);
    partitionList.add(2);
    workerInfo.put(workerId, new ArrayList<Integer>());
    workerInfo.put(workerId2, partitionList);
    // second: 1 worker, the other worker die, this worker lock 3 paritions
    Map<String, List<Integer>> workerInfo2 = new HashMap<String, List<Integer>>();
    List<Integer> partitionList2 = new ArrayList<Integer>();
    partitionList2.add(3);
    partitionList2.add(1);
    workerInfo2.put(workerId, partitionList2);
    when(consumerClientMock.queryWorker(any(QueryWorkerRequest.class)))
        .thenReturn(new QueryWorkerResponse(workerInfo))
        .thenReturn(new QueryWorkerResponse(workerInfo2));
    // for renew/steal partition/re-balance
    // construct 2 times balance
    when(partitionFetcher1.isServing()).thenReturn(false).thenReturn(true);
    when(partitionFetcher3.isServing()).thenReturn(false).thenReturn(true);
    when(partitionFetcher0.isServing())
        .thenReturn(false).thenReturn(false).thenReturn(true);
    when(partitionFetcher2.isServing())
        .thenReturn(false).thenReturn(false).thenReturn(true);
    when(partitionFetcher4.isServing())
        .thenReturn(false).thenReturn(false).thenReturn(true);

    when(partitionFetcher1.isHoldingLock()).thenReturn(false).thenReturn(true);
    when(partitionFetcher3.isHoldingLock()).thenReturn(false).thenReturn(true);
    when(partitionFetcher0.isHoldingLock())
        .thenReturn(false).thenReturn(false).thenReturn(true);
    when(partitionFetcher2.isHoldingLock())
        .thenReturn(false).thenReturn(false).thenReturn(true);
    when(partitionFetcher4.isHoldingLock())
        .thenReturn(false).thenReturn(false).thenReturn(true);

    TalosConsumer consumer = new TalosConsumer(consumerGroup, consumerConfig,
        talosResourceName, workerId, new SimpleTopicAbnormalCallback(),
        consumerClientMock, talosAdminMock, fetcherMap);
    // sleep for 3 times balance
    Thread.sleep(1000);
    consumer.shutDown();
    LOG.info("[testWorkerOfflineLockLock] done");
  }

  // 6) partition increase: lock -> lock -> balance
  @Test
  public void testPartitionChangedLockLock() throws Exception {
    LOG.info("[testPartitionChangedLockLock] start");
    // partition check, change from 5 to 6
    when(talosAdminMock.describeTopic(any(DescribeTopicRequest.class)))
        .thenReturn(topic).thenReturn(topic3);
    // register self
    LockWorkerResponse lockWorkerResponse = new LockWorkerResponse(true);
    when(consumerClientMock.lockWorker(any(LockWorkerRequest.class)))
        .thenReturn(lockWorkerResponse);
    // renew check
    when(consumerClientMock.renew(any(RenewRequest.class)))
        .thenReturn(new RenewResponse(true, new ArrayList<Integer>()));
    // worker info check
    // first: 1 worker 5 partitions
    Map<String, List<Integer>> workerInfo = new HashMap<String, List<Integer>>();
    workerInfo.put(workerId, new ArrayList<Integer>());
    // second: 1 worker 6 partitions
    Map<String, List<Integer>> workerInfo2 = new HashMap<String, List<Integer>>();
    List<Integer> partitionList = new ArrayList<Integer>();
    for (int i = 0; i < partitionNum; ++i) {
      partitionList.add(i);
    }
    workerInfo2.put(workerId, partitionList);
    when(consumerClientMock.queryWorker(any(QueryWorkerRequest.class)))
        .thenReturn(new QueryWorkerResponse(workerInfo))
        .thenReturn(new QueryWorkerResponse(workerInfo2));
    // for renew/steal partition/re-balance
    // construct 2 times balance
    when(partitionFetcher0.isServing()).thenReturn(false).thenReturn(true);
    when(partitionFetcher1.isServing()).thenReturn(false).thenReturn(true);
    when(partitionFetcher2.isServing()).thenReturn(false).thenReturn(true);
    when(partitionFetcher3.isServing()).thenReturn(false).thenReturn(true);
    when(partitionFetcher4.isServing()).thenReturn(false).thenReturn(true);
    when(partitionFetcher5.isServing())
        .thenReturn(false).thenReturn(false).thenReturn(true);

    when(partitionFetcher0.isHoldingLock()).thenReturn(false).thenReturn(true);
    when(partitionFetcher1.isHoldingLock()).thenReturn(false).thenReturn(true);
    when(partitionFetcher2.isHoldingLock()).thenReturn(false).thenReturn(true);
    when(partitionFetcher3.isHoldingLock()).thenReturn(false).thenReturn(true);
    when(partitionFetcher4.isHoldingLock()).thenReturn(false).thenReturn(true);
    when(partitionFetcher5.isHoldingLock())
        .thenReturn(false).thenReturn(false).thenReturn(true);

    TalosConsumer consumer = new TalosConsumer(consumerGroup, consumerConfig,
        talosResourceName, workerId, new SimpleTopicAbnormalCallback(),
        consumerClientMock, talosAdminMock, fetcherMap);
    // sleep for 3 times balance
    Thread.sleep(1000);
    consumer.shutDown();
    LOG.info("[testPartitionChangedLockLock] done");
  }

  // 7) renew failed: lock -> unlock -> balance
  @Test
  public void testRenewFailedLockUnlock() throws Exception {
    LOG.info("[testRenewFailedLockUnlock] start");
    when(talosAdminMock.describeTopic(any(DescribeTopicRequest.class)))
        .thenReturn(topic);
    // register self
    LockWorkerResponse lockWorkerResponse = new LockWorkerResponse(true);
    when(consumerClientMock.lockWorker(any(LockWorkerRequest.class)))
        .thenReturn(lockWorkerResponse);
    // renew check
    List<Integer> failedList = new ArrayList<Integer>();
    failedList.add(0);
    when(consumerClientMock.renew(any(RenewRequest.class)))
        .thenReturn(new RenewResponse(true, new ArrayList<Integer>()))
        .thenReturn(new RenewResponse(true, failedList));
    // worker info check
    // first: 1 worker 5 partitions
    Map<String, List<Integer>> workerInfo = new HashMap<String, List<Integer>>();
    List<Integer> partitionList = new ArrayList<Integer>();
    for (int i = 0; i < partitionNum; ++i) {
      partitionList.add(i);
    }
    workerInfo.put(workerId, partitionList);
    when(consumerClientMock.queryWorker(any(QueryWorkerRequest.class)))
        .thenReturn(new QueryWorkerResponse(workerInfo));
    // for renew/steal partition/re-balance
    when(partitionFetcher0.isServing()).thenReturn(true);
    when(partitionFetcher1.isServing()).thenReturn(true);
    when(partitionFetcher2.isServing()).thenReturn(true);
    when(partitionFetcher3.isServing()).thenReturn(true);
    when(partitionFetcher4.isServing()).thenReturn(true);

    when(partitionFetcher0.isHoldingLock()).thenReturn(true).thenReturn(false);
    when(partitionFetcher1.isHoldingLock()).thenReturn(true);
    when(partitionFetcher2.isHoldingLock()).thenReturn(true);
    when(partitionFetcher3.isHoldingLock()).thenReturn(true);
    when(partitionFetcher4.isHoldingLock()).thenReturn(true);

    TalosConsumer consumer = new TalosConsumer(consumerGroup, consumerConfig,
        talosResourceName, workerId, new SimpleTopicAbnormalCallback(),
        consumerClientMock, talosAdminMock, fetcherMap);
    // sleep for 3 times renew
    Thread.sleep(800);
    consumer.shutDown();
    LOG.info("[testRenewFailedLockUnlock] done");
  }

  // 8) renew heartbeat failed: cancel all task
  @Test
  public void testRenewHeartbeatFailed() throws Exception {
    LOG.info("[testRenewHeartbeatFailed] start");
    // partition check
    when(talosAdminMock.describeTopic(any(DescribeTopicRequest.class)))
        .thenReturn(topic);
    // register self
    LockWorkerResponse lockWorkerResponse = new LockWorkerResponse(true);
    when(consumerClientMock.lockWorker(any(LockWorkerRequest.class)))
        .thenReturn(lockWorkerResponse);
    // renew check
    when(consumerClientMock.renew(any(RenewRequest.class)))
        .thenReturn(new RenewResponse(true, new ArrayList<Integer>()))
        .thenReturn(new RenewResponse(false, new ArrayList<Integer>()));
    // worker info check
    // first: 1 worker 5 partitions, then renew failed
    Map<String, List<Integer>> workerInfo = new HashMap<String, List<Integer>>();
    workerInfo.put(workerId, new ArrayList<Integer>());
    when(consumerClientMock.queryWorker(any(QueryWorkerRequest.class)))
        .thenReturn(new QueryWorkerResponse(workerInfo));
    // for renew/steal partition/re-balance
    when(partitionFetcher0.isServing()).thenReturn(true);
    when(partitionFetcher1.isServing()).thenReturn(true);
    when(partitionFetcher2.isServing()).thenReturn(true);
    when(partitionFetcher3.isServing()).thenReturn(true);
    when(partitionFetcher4.isServing()).thenReturn(true);

    when(partitionFetcher0.isHoldingLock()).thenReturn(true);
    when(partitionFetcher1.isHoldingLock()).thenReturn(true);
    when(partitionFetcher2.isHoldingLock()).thenReturn(true);
    when(partitionFetcher3.isHoldingLock()).thenReturn(true);
    when(partitionFetcher4.isHoldingLock()).thenReturn(true);

    TalosConsumer consumer = new TalosConsumer(consumerGroup, consumerConfig,
        talosResourceName, workerId, new SimpleTopicAbnormalCallback(),
        consumerClientMock, talosAdminMock, fetcherMap);
    // sleep for 3 times renew
    Thread.sleep(800);
    consumer.shutDown();
    LOG.info("[testRenewHeartbeatFailed] done");
  }

  // 9) describeTopic not exist error
  @Test
  public void testDescribeTopicNotExist() throws Exception {
    LOG.info("[testDescribeTopicNotExist] start");
    // partition check
    doReturn(topic).doThrow(new GalaxyTalosException().setErrorCode(
        ErrorCode.TOPIC_NOT_EXIST)).when(talosAdminMock).describeTopic(
        any(DescribeTopicRequest.class));
    // register self
    LockWorkerResponse lockWorkerResponse = new LockWorkerResponse(true);
    when(consumerClientMock.lockWorker(any(LockWorkerRequest.class)))
        .thenReturn(lockWorkerResponse);
    // renew check
    when(consumerClientMock.renew(any(RenewRequest.class)))
        .thenReturn(new RenewResponse(true, new ArrayList<Integer>()));
    Map<String, List<Integer>> workerInfo = new HashMap<String, List<Integer>>();
    workerInfo.put(workerId, new ArrayList<Integer>());
    when(consumerClientMock.queryWorker(any(QueryWorkerRequest.class)))
        .thenReturn(new QueryWorkerResponse(workerInfo));

    TalosConsumer consumer = new TalosConsumer(consumerGroup, consumerConfig,
        talosResourceName, workerId, new SimpleTopicAbnormalCallback(),
        consumerClientMock, talosAdminMock, fetcherMap);
    // sleep for 3 times renew
    Thread.sleep(400);
    consumer.shutDown();
    LOG.info("[testDescribeTopicNotExist] done");
  }

  // 10) describeTopic resourceName changed, old topic deleted
  @Test
  public void testDescribeTopicResourceNameChanged() throws Exception {
    LOG.info("[testDescribeTopicResourceNameChanged] start");
    // partition check
    when(talosAdminMock.describeTopic(any(DescribeTopicRequest.class)))
        .thenReturn(topic).thenReturn(topic2);
    // register self
    LockWorkerResponse lockWorkerResponse = new LockWorkerResponse(true);
    when(consumerClientMock.lockWorker(any(LockWorkerRequest.class)))
        .thenReturn(lockWorkerResponse);
    // renew check
    when(consumerClientMock.renew(any(RenewRequest.class)))
        .thenReturn(new RenewResponse(true, new ArrayList<Integer>()));
    Map<String, List<Integer>> workerInfo = new HashMap<String, List<Integer>>();
    workerInfo.put(workerId, new ArrayList<Integer>());
    when(consumerClientMock.queryWorker(any(QueryWorkerRequest.class)))
        .thenReturn(new QueryWorkerResponse(workerInfo));

    TalosConsumer consumer = new TalosConsumer(consumerGroup, consumerConfig,
        talosResourceName, workerId, new SimpleTopicAbnormalCallback(),
        consumerClientMock, talosAdminMock, fetcherMap);
    // sleep for 3 times renew
    Thread.sleep(400);
    consumer.shutDown();
    LOG.info("[testDescribeTopicResourceNameChanged] done");
  }

}
