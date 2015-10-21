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
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import libthrift091.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.infra.galaxy.rpc.thrift.Credential;
import com.xiaomi.infra.galaxy.talos.admin.TalosAdmin;
import com.xiaomi.infra.galaxy.talos.client.TalosClientFactory;
import com.xiaomi.infra.galaxy.talos.client.Utils;
import com.xiaomi.infra.galaxy.talos.thrift.CheckPoint;
import com.xiaomi.infra.galaxy.talos.thrift.ConsumeUnit;
import com.xiaomi.infra.galaxy.talos.thrift.ConsumerService;
import com.xiaomi.infra.galaxy.talos.thrift.LockPartitionRequest;
import com.xiaomi.infra.galaxy.talos.thrift.LockPartitionResponse;
import com.xiaomi.infra.galaxy.talos.thrift.MessageAndOffset;
import com.xiaomi.infra.galaxy.talos.thrift.QueryOffsetRequest;
import com.xiaomi.infra.galaxy.talos.thrift.QueryOffsetResponse;
import com.xiaomi.infra.galaxy.talos.thrift.QueryWorkerRequest;
import com.xiaomi.infra.galaxy.talos.thrift.QueryWorkerResponse;
import com.xiaomi.infra.galaxy.talos.thrift.RenewRequest;
import com.xiaomi.infra.galaxy.talos.thrift.RenewResponse;
import com.xiaomi.infra.galaxy.talos.thrift.Topic;
import com.xiaomi.infra.galaxy.talos.thrift.TopicAndPartition;
import com.xiaomi.infra.galaxy.talos.thrift.TopicTalosResourceName;
import com.xiaomi.infra.galaxy.talos.thrift.UnlockPartitionRequest;
import com.xiaomi.infra.galaxy.talos.thrift.UpdateOffsetRequest;
import com.xiaomi.infra.galaxy.talos.thrift.UpdateOffsetResponse;

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
        return;
      }

      if (!topicTalosResourceName.equals(
          topic.getTopicInfo().getTopicTalosResourceName())) {
        LOG.error("The topic: " + topicTalosResourceName.getTopicTalosResourceName() +
            " not exist. It might have been deleted. " +
            "The getMessage threads will be cancel.");
        List<Integer> toCancelList = new ArrayList<Integer>(
            servingPartitionMap.keySet());
        releasePartitionLock(toCancelList);
        cachedExecutor.shutdown();
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
   * 'CheckPartitionTask' takes charge of updating partitionNumber -- synchronized
   * 'CheckWorkerInfoTask' takes charge of updating workerInfoMap -- synchronized
   */
  private class ReBalanceTask implements Runnable {

    @Override
    public void run() {
      makeBalance();
    }
  } // ReBalanceTask


  /**
   * ReNew Task
   *
   * periodically renew the worker heartbeat and the serving partitions lock.
   * Note the data of 'workerInfoMap' may be later than 'servingPartitionMap',
   * so we take 'servingPartitionMap' as guideline
   */
  private class ReNewTask implements Runnable {

    @Override
    public void run() {
      List<Integer> servingPartitionList = new ArrayList<Integer>(
          servingPartitionMap.keySet());
      ConsumeUnit consumeUnit = new ConsumeUnit(consumerGroup,
          topicTalosResourceName, servingPartitionList, workerId);
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

        // TODO: renew check heartbeat success or failed and do something
        if (renewResponse.getFailedPartitionListSize() == 0) {
          LOG.info("The worker: " + workerId + " success renew partitions: " +
              servingPartitionList);
          return;
        }
      }

      // stop read, commit offset, unlock for renew failed partitions
      // the release process is graceful, so may be a long time,
      // so switch thread and do not block renew thread
      if (renewResponse != null && renewResponse.getFailedPartitionListSize() > 0) {
        List<Integer> failedRenewList = renewResponse.getFailedPartitionList();
        reBalanceExecutor.execute(new UnlockTask(failedRenewList));
      }
    }
  }

  /**
   * UnlockTask
   *
   * this task just used for renew failed and switch thread to release lock
   */
  private class UnlockTask implements Runnable {
    private List<Integer> toRelesseList;

    private UnlockTask(List<Integer> toRelesseList) {
      this.toRelesseList = toRelesseList;
    }

    @Override
    public void run() {
      releasePartitionLock(toRelesseList);
    }
  }


  /**
   * MessageProcessTask
   *
   * The runnable task for continuously reading messages
   * by SimpleConsumer.fetchMessage and commit the processed message offset
   */
  private class MessageProcessTask implements Runnable {
    private boolean isShutDown;
    private long startOffset;
    private MessageProcessor messageProcessor;
    private SimpleConsumer simpleConsumer;

    private MessageProcessTask(MessageProcessor messageProcessor,
        SimpleConsumer simpleConsumer, long startOffset) {
      isShutDown = false;
      this.startOffset = startOffset;
      this.messageProcessor = messageProcessor;
      this.simpleConsumer = simpleConsumer;
    }

    // for cancel reading data from this partition
    private void shutDown() {
      isShutDown = true;
    }

    // commit the last processed offset and update the startOffset
    private void commitOffset(long offset) throws TException {
      CheckPoint checkPoint = new CheckPoint(consumerGroup,
          simpleConsumer.getTopicAndPartition(), offset, workerId);
      UpdateOffsetRequest updateOffsetRequest = new UpdateOffsetRequest(checkPoint);
      UpdateOffsetResponse updateOffsetResponse = consumerClient.updateOffset(
          updateOffsetRequest);
      // update startOffset as next message
      if (updateOffsetResponse.isSuccess()) {
        startOffset = offset + 1;
      }
    }

    @Override
    public void run() {
      LOG.info("The MessageProcessTask for topic: " +
          simpleConsumer.getTopicTalosResourceName().getTopicTalosResourceName() +
          " partition: " + simpleConsumer.getPartitionId() + " start");

      while (!isShutDown) {
        try {
          List<MessageAndOffset> messageList = simpleConsumer.fetchMessage(startOffset);
          messageProcessor.process(messageList);
          // commit the last message offset to consumer state table
          commitOffset(messageList.get(messageList.size() - 1).getMessageOffset());
        } catch (Throwable e) {
          LOG.error("Error: " + e.toString() + " when getting messages from topic: " +
              simpleConsumer.getTopicTalosResourceName().getTopicTalosResourceName() +
              " partition: " + simpleConsumer.getPartitionId());
        }
      }

      LOG.info("The MessageProcessTask for topic: " +
          simpleConsumer.getTopicTalosResourceName().getTopicTalosResourceName() +
          " partition: " + simpleConsumer.getPartitionId() + " is shutdown");
    }
  } // MessageProcessTask

  /**
   * The ProcessTaskAndFuture class used for
   * 1) messageProcessTask.shutdown() used to gracefully stop MessageProcessTask
   * 2) future.get() used to wait for MessageProcessTask finish
   * and then release the partition lock
   */
  private class ProcessTaskAndFuture {
    private MessageProcessTask messageProcessTask;
    private Future future;

    private ProcessTaskAndFuture(MessageProcessTask messageProcessTask, Future future) {
      this.messageProcessTask = messageProcessTask;
      this.future = future;
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(TalosConsumer.class);

  /**
   * the attribute of 'has'/'min'/'max' respectively represents:
   * has: the consumer worker is serving partition number now
   * min: the minimum partition number of the worker should serving
   * max: the maximum partition number of the worker should serving
   *
   * has = the partition number of worker is serving
   * min = partitions / worker_count
   * max = partitions / worker_count(+ 1 if worker_count can't be divided)
   *
   * 1) steal lock conditions
   * if 'has' < 'min' || ('has' < 'max' && 'exist idle partition')
   * where 'idle partition' means to read all served partition id
   * of the topic from HBase and make diff with partition number, diff is not null.
   * try to steal the diff until the conditions break;
   *
   * 2) release lock conditions
   * if 'has' > 'max' || ('has' > 'min' && 'exist idle worker' && 'workerNumber <= partitionNumber')
   * where 'idle worker' means some workers are not serving any partition.
   * idle workers wait other to release some locks and try to steal them
   *
   * 3) invoke re-balancer conditions
   * when the reBalancer is invoked, re-calculate the 'has'/'min'/'max' and try to
   * steal/release locks. The invoked conditions are as follows:
   * a) partitionCheckTask: when partition number change
   * b) workerInfoCheckTask: this task try to check
   * the above steal/release conditions every time which guarantee the worker
   * can achieve its lock eventually.
   */
  private int has;
  private int min;
  private int max;

  // scheduledExecutor only has 3 threads used for
  // one thread for checking partition number periodically
  // one thread for checking alive worker info periodically
  // one thread for renew worker heartbeat and serving partition locks periodically
  private ScheduledExecutorService scheduledExecutor;

  // cachedExecutor is a thread pool for storing worker threads and
  // creating new threads as needed as well as recycling idle threads
  private ExecutorService cachedExecutor;

  // reBalanceExecutor is a single thread pool and
  // execute re-balance task one by one
  private ExecutorService reBalanceExecutor;

  private TalosConsumerConfig talosConsumerConfig;
  private TalosClientFactory talosClientFactory;
  private TalosAdmin talosAdmin;
  private ConsumerService.Iface consumerClient;
  private String topicName;
  private TopicTalosResourceName topicTalosResourceName;
  private int partitionNumber;
  private Map<String, List<Integer>> workerInfoMap;
  private Map<Integer, ProcessTaskAndFuture> servingPartitionMap;
  private MessageProcessorFactory messageProcessorFactory;
  private String consumerGroup;
  private String workerId;
  private Random random;

  public TalosConsumer(String consumerGroup, TalosConsumerConfig consumerConfig,
      TopicTalosResourceName topicTalosResourceName,
      MessageProcessorFactory messageProcessorFactory) throws TException {
    this(consumerGroup, consumerConfig, new Credential(),
        topicTalosResourceName, messageProcessorFactory);
  }

  public TalosConsumer(String consumerGroup, TalosConsumerConfig consumerConfig,
      Credential credential, TopicTalosResourceName topicTalosResourceName,
      MessageProcessorFactory messageProcessorFactory) throws TException {
    this(consumerGroup, consumerConfig, credential, topicTalosResourceName,
        messageProcessorFactory, "");
  }

  public TalosConsumer(String consumerGroup, TalosConsumerConfig consumerConfig,
      Credential credential, TopicTalosResourceName topicTalosResourceName,
      MessageProcessorFactory messageProcessorFactory, String clientIdPrefix)
      throws TException {
    this.consumerGroup = consumerGroup;
    this.messageProcessorFactory = messageProcessorFactory;
    servingPartitionMap = new HashMap<Integer, ProcessTaskAndFuture>();
    talosConsumerConfig = consumerConfig;
    workerId = Utils.generateClientId(clientIdPrefix);
    random = new Random();
    scheduledExecutor = Executors.newScheduledThreadPool(3);
    cachedExecutor = Executors.newCachedThreadPool();
    reBalanceExecutor = Executors.newSingleThreadExecutor();
    talosClientFactory = new TalosClientFactory(talosConsumerConfig, credential);
    talosAdmin = new TalosAdmin(talosClientFactory);
    consumerClient = talosClientFactory.newConsumerClient();
    has = min = max = 0;

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
    partitionNumber = topic.getTopicAttribute().getPartitionNumber();
    this.topicTalosResourceName = topicTalosResourceName;
    LOG.info("The worker: " + workerId + " check and get topic info done");
  }

  /**
   * Renew contains two sections:
   * 1) worker heartbeat
   * 2) serving partitions lock
   */
  private void registerSelf() throws TException {
    ConsumeUnit consumeUnit = new ConsumeUnit(consumerGroup,
        topicTalosResourceName, new ArrayList<Integer>(), workerId);
    RenewRequest renewRequest = new RenewRequest(consumeUnit);
    RenewResponse renewResponse = consumerClient.renew(renewRequest);

    int maxRetry = talosConsumerConfig.getSelfRegisterMaxRetry();
    while (maxRetry-- > 0) {
      if (renewResponse.isHeartbeatSuccess()) {
        LOG.info("The worker: " + workerId + " register self success");
        return;
      }
      LOG.warn("The worker: " + workerId + " register self failed, make retry");
      renewResponse = consumerClient.renew(renewRequest);
    }
    LOG.error("The worker: " + workerId + " register self failed");
    throw new RuntimeException(workerId + " register self failed");
  }

  private synchronized void getWorkerInfo() throws TException {
    QueryWorkerRequest queryWorkerRequest = new QueryWorkerRequest(
        consumerGroup, topicTalosResourceName);
    QueryWorkerResponse queryWorkerResponse = consumerClient.queryWorker(
        queryWorkerRequest);
    workerInfoMap = queryWorkerResponse.getWorkerMap();
  }

  private void makeBalance() {
    // calculate 'has'
    has = 0;
    if (workerInfoMap.containsKey(workerId)) {
      has = workerInfoMap.get(workerId).size();
    }

    // calculate 'min'/'max'
    Preconditions.checkArgument(partitionNumber > 0);
    if (workerInfoMap.size() <= 0) {
      min = max = partitionNumber;
    } else if (partitionNumber < workerInfoMap.size()) {
      min = max = 1;
    } else {
      min = max = partitionNumber / workerInfoMap.size();
      if (partitionNumber % workerInfoMap.size() != 0) {
        max += 1;
      }
    }

    List<Integer> toStealList = new ArrayList<Integer>();
    List<Integer> toReleaseList = new ArrayList<Integer>();

    // stealing judgement
    List<Integer> idlePartitions = getIdlePartitions();
    if (idlePartitions.size() > 0) {
      Preconditions.checkArgument(max >= has);
      int toStealNum = max - has;
      while (toStealNum-- > 0 && idlePartitions.size() > 0) {
        // get a random one
        int randomIndex = random.nextInt(idlePartitions.size());
        toStealList.add(idlePartitions.remove(randomIndex));
      }
    }

    // release judgement
    int toReleaseNum = 0;
    if (has > max) {
      toReleaseNum = has - max;
    } else if (has > min && existIdleWorker()) {
      toReleaseNum = has - min;
    }
    // get to release list
    if (toReleaseNum > 0) {
      Preconditions.checkArgument(workerInfoMap.containsKey(workerId));
      List<Integer> hasList = workerInfoMap.get(workerId);
      while (toReleaseNum-- > 0 && hasList.size() > 0) {
        toReleaseList.add(hasList.remove(0));
      }
    }

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
    // try to lock
    ConsumeUnit consumeUnit = new ConsumeUnit(consumerGroup,
        topicTalosResourceName, toStealList, workerId);
    LockPartitionRequest lockRequest = new LockPartitionRequest(consumeUnit);

    // TODO: if lock fail, retry a few times
    LockPartitionResponse lockResponse = null;
    try {
      lockResponse = consumerClient.lockPartition(lockRequest);
    } catch (TException e) {
      LOG.error("Worker: " + workerId + " steal partition error: " + e.toString());
      return;
    }

    // read offset, start read
    List<Integer> servingPartitionList = lockResponse.getSuccessPartitions();
    if (servingPartitionList.size() > 0) {
      LOG.info("Worker: " + workerId + " success to lock partitions: " +
          servingPartitionList);
    }

    // init serving partition MessageProcessTask
    for (Integer partitionId : servingPartitionList) {
      Preconditions.checkArgument(!servingPartitionMap.containsKey(partitionId));
      TopicAndPartition topicAndPartition = new TopicAndPartition(topicName,
          topicTalosResourceName, partitionId);
      QueryOffsetRequest queryOffsetRequest = new QueryOffsetRequest(
          consumerGroup, topicAndPartition);
      QueryOffsetResponse queryOffsetResponse = null;
      try {
        queryOffsetResponse = consumerClient.queryOffset(queryOffsetRequest);
      } catch (TException e) {
        LOG.error("Worker: " + workerId + " query partition offset error: " +
            e.toString() + " skip it");
        continue;
      }

      // startOffset = queryOffset + 1
      long startOffset = queryOffsetResponse.getMsgOffset() + 1;
      SimpleConsumer simpleConsumer = new SimpleConsumer(talosConsumerConfig,
          topicAndPartition, talosClientFactory.newMessageClient());
      MessageProcessTask messageProcessTask = new MessageProcessTask(
          messageProcessorFactory.createProcessor(), simpleConsumer, startOffset);
      Future future = cachedExecutor.submit(messageProcessTask);
      servingPartitionMap.put(partitionId, new ProcessTaskAndFuture(
          messageProcessTask, future));
    }

    List<Integer> failedPartitionList = lockResponse.getFailedPartitions();
    if (failedPartitionList.size() > 0) {
      LOG.warn("Worker: " + workerId + " failed to lock partitions: " +
          failedPartitionList);
    }
  }

  private void releasePartitionLock(List<Integer> toReleaseList) {
    LOG.info("Worker: " + workerId + " try to release partition: " + toReleaseList);
    // stop read, commit offset
    for (Integer partitionId : toReleaseList) {
      Preconditions.checkArgument(servingPartitionMap.containsKey(partitionId));
      ProcessTaskAndFuture taskAndFuture = servingPartitionMap.get(partitionId);
      // gracefully stop read and commit offset, then quit
      taskAndFuture.messageProcessTask.shutDown();
      // wait task quit gracefully
      try {
        taskAndFuture.future.get();
      } catch (Throwable e) {
        // if quit not graceful, do nothing
      }
      // remove local task
      servingPartitionMap.remove(partitionId);
    }

    // release lock, if unlock failed, we just wait ttl work.
    ConsumeUnit consumeUnit = new ConsumeUnit(consumerGroup,
        topicTalosResourceName, toReleaseList, workerId);
    UnlockPartitionRequest unlockRequest = new UnlockPartitionRequest(consumeUnit);
    try {
      consumerClient.unlockPartition(unlockRequest);
    } catch (Throwable e) {
      LOG.warn("Worker: " + workerId + " release partition error: " + e.toString());
      return;
    }
    LOG.info("Success to release partition: " + toReleaseList);
  }

  private void initCheckPartitionTask() {
    // check and update partition number every 1 minutes delay by default
    scheduledExecutor.scheduleWithFixedDelay(new CheckPartitionTask(),
        talosConsumerConfig.getPartitionCheckInterval(),
        talosConsumerConfig.getPartitionCheckInterval(), TimeUnit.MILLISECONDS);
  }

  private void initCheckWorkerInfoTask() {
    scheduledExecutor.scheduleWithFixedDelay(new CheckWorkerInfoTask(),
        talosConsumerConfig.getWorkerInfoCheckInterval(),
        talosConsumerConfig.getWorkerInfoCheckInterval(), TimeUnit.MILLISECONDS);
  }

  private void initRenewTask() {
    scheduledExecutor.scheduleAtFixedRate(new ReNewTask(),
        talosConsumerConfig.getReNewCheckInterval(),
        talosConsumerConfig.getReNewCheckInterval(), TimeUnit.MILLISECONDS);
  }

  private synchronized void setPartitionNumber(int partitionNumber) {
    this.partitionNumber = partitionNumber;
  }

  private synchronized List<Integer> getIdlePartitions() {
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
    return idlePartitions;
  }

  private synchronized boolean existIdleWorker() {
    int idleCount = 0;
    for (List<Integer> partitionList : workerInfoMap.values()) {
      if (partitionList.size() == 0) {
        idleCount++;
      }
    }
    if (partitionNumber >= workerInfoMap.size() && idleCount > 0) {
      return true;
    }
    if (partitionNumber < workerInfoMap.size() &&
        (idleCount > workerInfoMap.size() - partitionNumber)) {
      return true;
    }
    return false;
  }
}
