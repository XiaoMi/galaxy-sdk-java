/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: yongxing@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.consumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.base.Preconditions;
import libthrift091.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.infra.galaxy.rpc.thrift.Credential;
import com.xiaomi.infra.galaxy.talos.admin.TalosAdmin;
import com.xiaomi.infra.galaxy.talos.client.TalosClientFactory;
import com.xiaomi.infra.galaxy.talos.client.TopicAbnormalCallback;
import com.xiaomi.infra.galaxy.talos.client.Utils;
import com.xiaomi.infra.galaxy.talos.thrift.ConsumeUnit;
import com.xiaomi.infra.galaxy.talos.thrift.ConsumerService;
import com.xiaomi.infra.galaxy.talos.thrift.LockWorkerRequest;
import com.xiaomi.infra.galaxy.talos.thrift.LockWorkerResponse;
import com.xiaomi.infra.galaxy.talos.thrift.QueryWorkerRequest;
import com.xiaomi.infra.galaxy.talos.thrift.QueryWorkerResponse;
import com.xiaomi.infra.galaxy.talos.thrift.RenewRequest;
import com.xiaomi.infra.galaxy.talos.thrift.RenewResponse;
import com.xiaomi.infra.galaxy.talos.thrift.Topic;
import com.xiaomi.infra.galaxy.talos.thrift.TopicTalosResourceName;

public class TalosConsumer {
  /**
   * Check Partition Task
   *
   * if partition number change, invoke ReBalanceTask
   */
  private class CheckPartitionTask implements Runnable {

    @Override
    public void run() {
      Topic topic;
      try {
        topic = talosAdmin.describeTopic(topicName);
      } catch (Throwable throwable) {
        LOG.error("Exception in CheckPartitionTask: " + throwable.toString());
        // if throwable instance of HBaseOperationFailed, just return
        // if throwable instance of TopicNotExist, cancel all reading task
        if (Utils.isTopicNotExist(throwable)) {
          cancelAllConsumingTask();
          topicAbnormalCallback.abnormalHandler(topicTalosResourceName, throwable);
        }
        return;
      }

      if (!topicTalosResourceName.equals(
          topic.getTopicInfo().getTopicTalosResourceName())) {
        String errMsg = "The topic: " +
            topicTalosResourceName.getTopicTalosResourceName() +
            " not exist. It might have been deleted. " +
            "The getMessage threads will be cancel.";
        LOG.error(errMsg);
        cancelAllConsumingTask();
        topicAbnormalCallback.abnormalHandler(topicTalosResourceName,
            new Throwable(errMsg));
        return;
      }

      int topicPartitionNum = topic.getTopicAttribute().getPartitionNumber();
      if (partitionNumber < topicPartitionNum) {
        // update partition number and call the re-balance
        setPartitionNumber(topicPartitionNum);
        // call the re-balance task
        reBalanceExecutor.execute(new ReBalanceTask());
      }
    }
  } // checkPartitionTask


  /**
   * Check Worker Info Task
   *
   * check alive worker number and get the worker serving map
   * 1) get the latest worker info and synchronized update the local workInfoMap
   * 2) invoke the ReBalanceTask every time
   *
   * Note:
   * a) current alive workers refer to scan 'consumerGroup+Topic+Worker'
   * b) all serving partitions got by the a)'s alive workers
   *
   * G+T+W    G+T+P
   * yes       no  -- normal, exist idle workers
   * no        yes -- abnormal, but ttl will fix it
   */
  private class CheckWorkerInfoTask implements Runnable {

    @Override
    public void run() {
      try {
        getWorkerInfo();
      } catch (Throwable e) {
        LOG.error("Get worker info error: " + e.toString());
      }
      // invoke the re-balance task every time
      reBalanceExecutor.execute(new ReBalanceTask());
    }
  }


  /**
   * Re-Balance Task
   *
   * This task just re-calculate the 'has'/'max'/'min' and try to steal/release
   * 'CheckPartitionTask' takes charge of updating partitionNumber
   * 'CheckWorkerInfoTask' takes charge of updating workerInfoMap
   */
  private class ReBalanceTask implements Runnable {

    @Override
    public void run() {
      makeBalance();
    }
  } // ReBalanceTask


  /**
   * ReNew Task (contains two sections per renew)
   *
   * Note: we make renew process outside rather than inner PartitionFetcher class
   * because:
   * 1) make the partitionFetcher heartbeat and worker heartbeat together
   * 2) renew all the serving partitions lock within one rpc process,
   *    which prevent massive rpc request to server
   *
   * when get what to renew, we take 'partitionFetcherMap' as guideline
   */
  private class ReNewTask implements Runnable {

    private List<Integer> getRenewPartitionList() {
      List<Integer> toRenewList = new ArrayList<Integer>();
      readWriteLock.readLock().lock();
      for (Map.Entry<Integer, PartitionFetcher> entry :
          partitionFetcherMap.entrySet()) {
        if (entry.getValue().isHoldingLock()) {
          toRenewList.add(entry.getKey());
        }
      }
      readWriteLock.readLock().unlock();
      return toRenewList;
    }

    @Override
    public void run() {
      List<Integer> toRenewPartitionList = getRenewPartitionList();
      ConsumeUnit consumeUnit = new ConsumeUnit(consumerGroup,
          topicTalosResourceName, toRenewPartitionList, workerId);
      RenewRequest renewRequest = new RenewRequest(consumeUnit);
      RenewResponse renewResponse = null;

      // plus 1 to include the first renew operation
      int maxRetry = talosConsumerConfig.getReNewMaxRetry() + 1;
      while (maxRetry-- > 0) {
        try {
          renewResponse = consumerClient.renew(renewRequest);
        } catch (Throwable e) {
          LOG.error("Worker: " + workerId + " renew error: " + e.toString());
          continue;
        }

        // 1) make heartbeat success and renew partitions success
        if (renewResponse.isHeartbeatSuccess() &&
            renewResponse.getFailedPartitionListSize() == 0) {
          LOG.info("The worker: " + workerId + " success renew partitions: " +
              toRenewPartitionList);
          return;
        }
      }

      // 2) make heart beat failed, cancel all partitions
      // no need to renew anything, so block the renew thread and cancel all task
      if (renewResponse != null && !renewResponse.isHeartbeatSuccess()) {
        LOG.error("The worker: " + workerId +
            " failed to make heartbeat, cancel all consumer task");
        cancelAllConsumingTask();
      }

      // 3) make heartbeat success but renew some partitions failed
      // stop read, commit offset, unlock for renew failed partitions
      // the release process is graceful, so may be a long time,
      // do not block the renew thread and switch thread to re-balance thread
      if (renewResponse != null && renewResponse.getFailedPartitionListSize() > 0) {
        List<Integer> failedRenewList = renewResponse.getFailedPartitionList();
        releasePartitionLock(failedRenewList);
      }
    }
  }

  private class WorkerPair implements Comparable<WorkerPair> {
    private String workerId;
    private int hasPartitionNum;

    private WorkerPair(String workerId, int hasPartitionNum) {
      this.workerId = workerId;
      this.hasPartitionNum = hasPartitionNum;
    }

    @Override
    public int compareTo(WorkerPair o) {
      int temp = o.hasPartitionNum - hasPartitionNum; // descending sort
      if (0 == temp) {
        return o.workerId.compareTo(workerId);
      }
      return temp;
    }

    @Override
    public String toString() {
      return "{'" + workerId + '\'' + ", " + hasPartitionNum + '}';
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(TalosConsumer.class);

  private String workerId;
  private Random random;
  private String consumerGroup;
  private MessageProcessorFactory messageProcessorFactory;
  private Map<Integer, PartitionFetcher> partitionFetcherMap;
  private TalosConsumerConfig talosConsumerConfig;
  private TalosClientFactory talosClientFactory;
  private TalosAdmin talosAdmin;
  private ConsumerService.Iface consumerClient;
  private TopicAbnormalCallback topicAbnormalCallback;
  private ReadWriteLock readWriteLock;

  // 3 single scheduledExecutor respectively used for
  // a) checking partition number periodically
  // b) checking alive worker info periodically
  // c) renew worker heartbeat and serving partition locks periodically
  private ScheduledExecutorService partitionScheduledExecutor;
  private ScheduledExecutorService workerScheduleExecutor;
  private ScheduledExecutorService renewScheduleExecutor;

  // reBalanceExecutor is a single thread pool to execute re-balance task
  private ExecutorService reBalanceExecutor;

  // init by getting from rpc call as follows
  private String topicName;
  private int partitionNumber;
  private TopicTalosResourceName topicTalosResourceName;
  private Map<String, List<Integer>> workerInfoMap;

  public TalosConsumer(String consumerGroupName, TalosConsumerConfig consumerConfig,
      Credential credential, TopicTalosResourceName topicTalosResourceName,
      MessageProcessorFactory messageProcessorFactory, String clientIdPrefix,
      TopicAbnormalCallback abnormalCallback)
      throws TException {
    workerId = Utils.generateClientId(clientIdPrefix);
    random = new Random();
    consumerGroup = consumerGroupName;
    this.messageProcessorFactory = messageProcessorFactory;
    partitionFetcherMap = new ConcurrentHashMap<Integer, PartitionFetcher>();
    talosConsumerConfig = consumerConfig;
    talosClientFactory = new TalosClientFactory(talosConsumerConfig, credential);
    talosAdmin = new TalosAdmin(talosClientFactory);
    consumerClient = talosClientFactory.newConsumerClient();
    topicAbnormalCallback = abnormalCallback;
    readWriteLock = new ReentrantReadWriteLock();

    partitionScheduledExecutor = Executors.newSingleThreadScheduledExecutor();
    workerScheduleExecutor = Executors.newSingleThreadScheduledExecutor();
    renewScheduleExecutor = Executors.newSingleThreadScheduledExecutor();
    reBalanceExecutor = Executors.newSingleThreadExecutor();

    LOG.info("The worker: " + workerId + " is initializing...");
    // check and get topic info such as partitionNumber
    checkAndGetTopicInfo(topicTalosResourceName);
    // register self workerId
    registerSelf();
    // get worker info
    getWorkerInfo();
    // do balance and init simple consumer
    makeBalance();

    // start CheckPartitionTask/CheckWorkerInfoTask/RenewTask
    initCheckPartitionTask();
    initCheckWorkerInfoTask();
    initRenewTask();
  }

  public TalosConsumer(String consumerGroup, TalosConsumerConfig consumerConfig,
      TopicTalosResourceName topicTalosResourceName,
      MessageProcessorFactory messageProcessorFactory,
      TopicAbnormalCallback topicAbnormalCallback) throws TException {
    this(consumerGroup, consumerConfig, new Credential(),
        topicTalosResourceName, messageProcessorFactory, topicAbnormalCallback);
  }

  public TalosConsumer(String consumerGroup, TalosConsumerConfig consumerConfig,
      Credential credential, TopicTalosResourceName topicTalosResourceName,
      MessageProcessorFactory messageProcessorFactory,
      TopicAbnormalCallback topicAbnormalCallback) throws TException {
    this(consumerGroup, consumerConfig, credential, topicTalosResourceName,
        messageProcessorFactory, "", topicAbnormalCallback);
  }

  private void checkAndGetTopicInfo(TopicTalosResourceName topicTalosResourceName)
      throws TException {
    topicName = Utils.getTopicNameByResourceName(
        topicTalosResourceName.getTopicTalosResourceName());
    Topic topic = talosAdmin.describeTopic(topicName);

    if (!topicTalosResourceName.equals(
        topic.getTopicInfo().getTopicTalosResourceName())) {
      throw new IllegalArgumentException("The topic: " +
          topicTalosResourceName.getTopicTalosResourceName() + " not found");
    }
    setPartitionNumber(topic.getTopicAttribute().getPartitionNumber());
    this.topicTalosResourceName = topicTalosResourceName;
    LOG.info("The worker: " + workerId + " check and get topic info done");
  }

  private void registerSelf() throws TException {
    ConsumeUnit consumeUnit = new ConsumeUnit(consumerGroup,
        topicTalosResourceName, new ArrayList<Integer>(), workerId);
    LockWorkerRequest request = new LockWorkerRequest(consumeUnit);
    LockWorkerResponse lockWorkerResponse = null;

    int tryCount = talosConsumerConfig.getSelfRegisterMaxRetry() + 1;
    while (tryCount-- > 0) {
      lockWorkerResponse = consumerClient.lockWorker(request);
      if (lockWorkerResponse.isRegisterSuccess()) {
        LOG.info("The worker: " + workerId + " register self success");
        return;
      }
      LOG.warn("The worker: " + workerId + " register self failed, make retry");
    }
    LOG.error("The worker: " + workerId + " register self failed");
    throw new RuntimeException(workerId + " register self failed");
  }

  private void getWorkerInfo() throws TException {
    QueryWorkerRequest queryWorkerRequest = new QueryWorkerRequest(
        consumerGroup, topicTalosResourceName);
    QueryWorkerResponse queryWorkerResponse = consumerClient.queryWorker(
        queryWorkerRequest);

    // if queryWorkerInfoMap size equals 0,
    // it represents hbase failed error, do not update local map
    // because registration, the queryWorkerInfoMap size >= 1 at least
    if (queryWorkerResponse.getWorkerMapSize() == 0) {
      return;
    }
    readWriteLock.writeLock().lock();
    workerInfoMap = queryWorkerResponse.getWorkerMap();
    readWriteLock.writeLock().unlock();
  }

  private void calculateTargetList(int copyPartitionNum, int workerNumber,
      List<Integer> targetList) {
    if (workerNumber == 1) {
      // one worker serving all partitions
      targetList.add(copyPartitionNum);

    } else if (copyPartitionNum < workerNumber) {
      // per worker per partition, the extra worker must be idle
      for (int i = 0; i < copyPartitionNum; ++i) {
        targetList.add(1);
      }

    } else {
      // calculate the target sequence
      int min, remainder, sum = 0;
      min = copyPartitionNum / workerNumber;
      remainder = copyPartitionNum % workerNumber;
      // add max by remainder
      for (int i = 0; i < remainder; ++i) {
        targetList.add(min + 1);
        sum += (min + 1);
      }

      // add min by (workerNumber - remainder)
      for (int i = 0; i < (workerNumber - remainder); ++i) {
        targetList.add(min);
        sum += min;
      }
      Preconditions.checkArgument(sum == copyPartitionNum);
    }

    // sort target by descending
    Collections.sort(targetList, Collections.reverseOrder());
    LOG.info("calculate target partitions done: " + targetList);
  }

  private void calculateWorkerPairs(Map<String, List<Integer>> copyWorkerMap,
      List<WorkerPair> sortedWorkerPairs) {
    for (Map.Entry<String, List<Integer>> entry : copyWorkerMap.entrySet()) {
      sortedWorkerPairs.add(new WorkerPair(entry.getKey(), entry.getValue().size()));
    }
    Collections.sort(sortedWorkerPairs); // descending
    LOG.info("calculate sorted worker pairs: " + sortedWorkerPairs);
  }

  private void makeBalance() {
    /**
     * When start make balance, we deep copy 'partitionNumber' and 'workerInfoMap'
     * to prevent both value appear inconsistent during the process makeBalance
     */
    int copyPartitionNum = partitionNumber;
    Map<String, List<Integer>> copyWorkerInfoMap = deepCopyWorkerInfoMap();

    /**
     * if workerInfoMap not contains workerId, there must be error in renew task.
     * the renew task will cancel the consuming task and stop to read data,
     * so just return and do not care balance.
     */
    if (!copyWorkerInfoMap.containsKey(workerId)) {
      LOG.error("WorkerInfoMap not contains worker: " + workerId +
          ". There may be some error for renew task.");
      return;
    }

    // calculate target and sorted worker pairs
    List<Integer> targetList = new ArrayList<Integer>();
    List<WorkerPair> sortedWorkerPairs = new ArrayList<WorkerPair>();
    calculateTargetList(copyPartitionNum, copyWorkerInfoMap.size(), targetList);
    calculateWorkerPairs(copyWorkerInfoMap, sortedWorkerPairs);

    // judge stealing or release
    List<Integer> toStealList = new ArrayList<Integer>();
    List<Integer> toReleaseList = new ArrayList<Integer>();

    for (int i = 0; i < sortedWorkerPairs.size(); ++i) {
      if (sortedWorkerPairs.get(i).workerId.equals(workerId)) {
        List<Integer> hasList = getHasList();
        int has = hasList.size();

        // workerNum > partitionNum, idle workers have no match target, do nothing
        if (i >= targetList.size()) {
          break;
        }
        int target = targetList.get(i);

        // a balanced state, do nothing
        if (has == target) {
          break;

        } else if (has > target) {
          // release partitions
          int toReleaseNum = has - target;
          while (toReleaseNum-- > 0 && hasList.size() > 0) {
            toReleaseList.add(hasList.remove(0));
          }

        } else {
          // stealing partitions
          List<Integer> idlePartitions = getIdlePartitions();
          if (idlePartitions.size() > 0) {
            int toStealnum = target - has;
            while (toStealnum-- > 0 && idlePartitions.size() > 0) {
              int randomIndex = random.nextInt(idlePartitions.size());
              toStealList.add(idlePartitions.remove(randomIndex));
            }
          }
        } // else
        break;
      } // if
    } // for

    // steal or release partition lock or reached a balance state
    Preconditions.checkArgument(!(toStealList.size() > 0 &&
        toReleaseList.size() > 0));
    if (toStealList.size() > 0) {
      stealPartitionLock(toStealList);
    } else if (toReleaseList.size() > 0) {
      releasePartitionLock(toReleaseList);
    } else {
      // do nothing when reach balance state
      LOG.info("The workers have reached a balance state.");
    }
  }

  private void stealPartitionLock(List<Integer> toStealList) {
    LOG.info("Worker: " + workerId + " try to steal partition: " + toStealList);
    // try to lock and invoke serving partition PartitionFetcher to 'LOCKED' state
    readWriteLock.writeLock().lock();
    for (Integer partitionId : toStealList) {
      if (!partitionFetcherMap.containsKey(partitionId)) {
        PartitionFetcher partitionFetcher = new PartitionFetcher(consumerGroup,
            topicName, topicTalosResourceName, partitionId, talosConsumerConfig,
            workerId, consumerClient, talosClientFactory.newMessageClient(),
            messageProcessorFactory.createProcessor());
        partitionFetcherMap.put(partitionId, partitionFetcher);
      }
      partitionFetcherMap.get(partitionId).lock();
    }
    readWriteLock.writeLock().unlock();
  }

  private void releasePartitionLock(List<Integer> toReleaseList) {
    LOG.info("Worker: " + workerId + " try to release partition: " + toReleaseList);
    // stop read, commit offset, unlock the partition async
    for (Integer partitionId : toReleaseList) {
      Preconditions.checkArgument(partitionFetcherMap.containsKey(partitionId));
      partitionFetcherMap.get(partitionId).unlock();
    }
  }

  private void initCheckPartitionTask() {
    // check and update partition number every 1 minutes delay by default
    partitionScheduledExecutor.scheduleWithFixedDelay(new CheckPartitionTask(),
        talosConsumerConfig.getPartitionCheckInterval(),
        talosConsumerConfig.getPartitionCheckInterval(), TimeUnit.MILLISECONDS);
  }

  private void initCheckWorkerInfoTask() {
    workerScheduleExecutor.scheduleWithFixedDelay(new CheckWorkerInfoTask(),
        talosConsumerConfig.getWorkerInfoCheckInterval(),
        talosConsumerConfig.getWorkerInfoCheckInterval(), TimeUnit.MILLISECONDS);
  }

  private void initRenewTask() {
    renewScheduleExecutor.scheduleAtFixedRate(new ReNewTask(),
        talosConsumerConfig.getReNewCheckInterval(),
        talosConsumerConfig.getReNewCheckInterval(), TimeUnit.MILLISECONDS);
  }

  private void setPartitionNumber(int partitionNum) {
    readWriteLock.writeLock().lock();
    partitionNumber = partitionNum;
    readWriteLock.writeLock().unlock();
  }

  private List<Integer> getIdlePartitions() {
    readWriteLock.readLock().lock();
    Preconditions.checkArgument(partitionNumber > 0);
    List<Integer> idlePartitions = new ArrayList<Integer>();
    for (int i = 1; i <= partitionNumber; ++i) {
      idlePartitions.add(i);
    }

    for (List<Integer> valueList : workerInfoMap.values()) {
      for (int partitionId : valueList) {
        idlePartitions.remove(new Integer(partitionId));
      }
    }
    readWriteLock.readLock().unlock();
    return idlePartitions;
  }

  private List<Integer> getHasList() {
    List<Integer> hasList = new ArrayList<Integer>();
    readWriteLock.readLock().lock();
    for (Map.Entry<Integer, PartitionFetcher> entry :
        partitionFetcherMap.entrySet()) {
      if (entry.getValue().isServing()) {
        hasList.add(entry.getKey());
      }
    }
    readWriteLock.readLock().unlock();
    return hasList;
  }

  private void cancelAllConsumingTask() {
    releasePartitionLock(getHasList());
  }

  private Map<String, List<Integer>> deepCopyWorkerInfoMap() {
    readWriteLock.readLock().lock();
    Map<String, List<Integer>> copyMap = new HashMap<String, List<Integer>>(
        workerInfoMap.size());
    for (Map.Entry<String, List<Integer>> entry : workerInfoMap.entrySet()) {
      copyMap.put(entry.getKey(), new ArrayList<Integer>(entry.getValue()));
    }
    readWriteLock.readLock().unlock();
    return copyMap;
  }
}
