/**
 * Copyright 2018, Xiaomi. All rights reserved. Author: zhangqian8@xiaomi.com
 */
package com.xiaomi.infra.galaxy.talos.client;

import java.util.HashMap;
import java.util.Map;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.DiscardPolicy;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import libthrift091.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.infra.galaxy.talos.thrift.GetScheduleInfoRequest;
import com.xiaomi.infra.galaxy.talos.thrift.MessageService;
import com.xiaomi.infra.galaxy.talos.thrift.TopicAndPartition;
import com.xiaomi.infra.galaxy.talos.thrift.TopicTalosResourceName;

public class ScheduleInfoCache {
  private class GetScheduleInfoTask implements Runnable {
    @Override
    public void run() {
      int maxRetry = talosClientConfig.getScheduleInfoMaxRetry() + 1;
      while (maxRetry-- > 0) {
        try {
          //get and update scheduleInfoMap
          getScheduleInfo(topicTalosResourceName);
          return;
        } catch (Throwable throwable) {
          LOG.error("Exception in GetScheduleInfoTask: ", throwable);
          // 1.if throwable instance of TopicNotExist, cancel all reading task
          // 2.if other throwable such as LockNotExist or HbaseOperationFailed, retry again
          // 3.if scheduleInfoMap didn't update success after maxRetry, just return and use
          //  old data, it may update next time or targeted update when Transfered.
          if (Utils.isTopicNotExist(throwable)) {
            return;
          }
        }
      }
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(ScheduleInfoCache.class);

  private TopicTalosResourceName topicTalosResourceName;
  private TalosClientFactory talosClientFactory;
  private Map<TopicAndPartition, String> scheduleInfoMap;
  private MessageService.Iface messageClient;

  private Map<String, MessageService.Iface> messageClientMap;
  private boolean isAutoLocation;
  private TalosClientConfig talosClientConfig;
  private ReadWriteLock readWriteLock;

  private ScheduledExecutorService GetScheduleInfoScheduleExecutor;
  private ExecutorService GetScheduleInfoExecutor;
  private static Map<TopicTalosResourceName, ScheduleInfoCache> scheduleInfoCacheMap =
      new HashMap<TopicTalosResourceName, ScheduleInfoCache>();

  private ScheduleInfoCache(TopicTalosResourceName topicTalosResourceName,
      TalosClientConfig talosClientConfig, MessageService.Iface messageClient,
      TalosClientFactory talosClientFactory) {
    this.topicTalosResourceName = topicTalosResourceName;
    this.talosClientConfig = talosClientConfig;
    this.isAutoLocation = talosClientConfig.isAutoLocation();
    this.readWriteLock = new ReentrantReadWriteLock();
    this.messageClient = messageClient;
    this.talosClientFactory = talosClientFactory;
    this.messageClientMap = new HashMap<String, MessageService.Iface>();

    // GetScheduleInfoScheduleExecutor for Schedule get work, cause ScheduledExecutorService
    // use DelayedWorkQueue storage its task, which is unbounded. To private OOM, use
    // GetScheduleInfoExecutor execute task when transfered, setting Queue size as 2.
    GetScheduleInfoScheduleExecutor = Executors.newSingleThreadScheduledExecutor();
    GetScheduleInfoExecutor = new ThreadPoolExecutor(1, 1,
        0L, TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<Runnable>(2), new DiscardPolicy());

    try {
      //get and update scheduleInfoMap
      getScheduleInfo(topicTalosResourceName);
    } catch (Throwable throwable) {
      LOG.error("Exception in GetScheduleInfoTask: ", throwable);
      if (Utils.isTopicNotExist(throwable)) {
        return;
      }
    }
    initGetScheduleInfoTask();
  }

  private ScheduleInfoCache(TopicTalosResourceName topicTalosResourceName,
      TalosClientConfig talosClientConfig, MessageService.Iface messageClient){
    this.topicTalosResourceName = topicTalosResourceName;
    this.messageClient = messageClient;
    this.talosClientConfig = talosClientConfig;
    this.isAutoLocation = false;
  }

  public static synchronized ScheduleInfoCache getScheduleInfoCache(TopicTalosResourceName
      topicTalosResourceName, TalosClientConfig talosClientConfig,
      MessageService.Iface messageClient, TalosClientFactory talosClientFactory) {
    if (scheduleInfoCacheMap.get(topicTalosResourceName) == null) {
      if (talosClientFactory == null) {
        // this case should not exist normally, only when interface of simpleAPI improper used
        scheduleInfoCacheMap.put(topicTalosResourceName, new ScheduleInfoCache(topicTalosResourceName,
            talosClientConfig, messageClient));
        if(LOG.isDebugEnabled()){
          LOG.debug("SimpleProducer or SimpleConsumer was built using improperly constructed function");
        }
      } else {
        scheduleInfoCacheMap.put(topicTalosResourceName, new ScheduleInfoCache(topicTalosResourceName,
            talosClientConfig, messageClient, talosClientFactory));
      }
    }
    return scheduleInfoCacheMap.get(topicTalosResourceName);
  }

  public MessageService.Iface getOrCreateMessageClient(TopicAndPartition topicAndPartition) {
      if (scheduleInfoMap == null){
        updatescheduleInfoCache();
        return this.messageClient;
      }
      String host = scheduleInfoMap.get(topicAndPartition);
      if (host == null) {
        updatescheduleInfoCache();
        return this.messageClient;
      }
      MessageService.Iface messageClient = messageClientMap.get(host);
      if (messageClient == null) {
        messageClient = talosClientFactory.newMessageClient("http://" + host);
        messageClientMap.put(host, messageClient);
      }
      return messageClient;
  }

  public void updatescheduleInfoCache() {
    if (isAutoLocation) {
      GetScheduleInfoExecutor.submit(new GetScheduleInfoTask());
    }
  }

  public void shutDown() {
    LOG.info("scheduleInfoCache is shutting down...");
    for (Map.Entry<TopicTalosResourceName, ScheduleInfoCache> entry : scheduleInfoCacheMap.entrySet()) {
      entry.getValue().GetScheduleInfoScheduleExecutor.shutdownNow();
      entry.getValue().GetScheduleInfoExecutor.shutdownNow();
    }
    LOG.info("scheduleInfoCache shutdown.");
  }

  private void getScheduleInfo(TopicTalosResourceName topicTalosResourceName)
      throws TException {
    // judge isAutoLocation serveral place to make sure request server only when need.
    // 1.before send Executor task make sure send Executor task when need;
    // 2.judge in getScheduleInfo is the Final guarantee good for code extendibility;
    if (isAutoLocation) {
      Map<TopicAndPartition, String> topicScheduleInfoMap =
          messageClient.getScheduleInfo(new GetScheduleInfoRequest(topicTalosResourceName))
              .getScheduleInfo();
      readWriteLock.writeLock().lock();
      scheduleInfoMap = topicScheduleInfoMap;
      readWriteLock.writeLock().unlock();
      if(LOG.isDebugEnabled()){
        LOG.debug("getScheduleInfo success" + scheduleInfoMap.toString());
      }
    }
  }

  private void initGetScheduleInfoTask() {
    if (isAutoLocation) {
      GetScheduleInfoScheduleExecutor.scheduleWithFixedDelay(new GetScheduleInfoTask(),
          talosClientConfig.getScheduleInfoInterval(),
          talosClientConfig.getScheduleInfoInterval(), TimeUnit.MILLISECONDS);
    }
  }
}
