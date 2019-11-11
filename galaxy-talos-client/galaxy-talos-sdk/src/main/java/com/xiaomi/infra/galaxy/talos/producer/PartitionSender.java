/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: yongxing@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.producer;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.infra.galaxy.talos.client.Constants;
import com.xiaomi.infra.galaxy.talos.client.NamedThreadFactory;
import com.xiaomi.infra.galaxy.talos.client.Utils;
import com.xiaomi.infra.galaxy.talos.thrift.Message;
import com.xiaomi.infra.galaxy.talos.thrift.MessageService;
import com.xiaomi.infra.galaxy.talos.thrift.TopicAndPartition;
import com.xiaomi.infra.galaxy.talos.thrift.TopicTalosResourceName;

public class PartitionSender {

  private class MessageCallbackTask implements Runnable {
    private UserMessageResult userMessageResult;

    private MessageCallbackTask(UserMessageResult userMessageResult) {
      this.userMessageResult = userMessageResult;
    }

    @Override
    public void run() {
      if (userMessageResult.isSuccessful()) {
        userMessageCallback.onSuccess(userMessageResult);
      } else {
        userMessageCallback.onError(userMessageResult);
      }
    }
  } // MessageCallbackTask

  private class MessageWriter implements Runnable {
    private SimpleProducer simpleProducer;
    private AtomicInteger failedCounter;

    private MessageWriter() {
      simpleProducer = new SimpleProducer(talosProducerConfig,
          topicAndPartition, messageClient, clientId, requestId);
      failedCounter = new AtomicInteger(0);
    }

    @Override
    public void run() {
      while (true) {
        try {
          List<Message> messageList =
              partitionMessageQueue.getMessageList();

          // when messageList return no message, this means TalosProducer not
          // alive and there is no more message to send , then we should exit
          // write message right now;
          if (messageList.isEmpty()) {
            // notify to wake up producer's global lock
            synchronized (globalLock) {
              globalLock.notifyAll();
            }
            break;
          }

          putMessage(messageList);


        } catch (Throwable throwable) {
          LOG.error("PutMessageTask for topicAndPartition: " +
              topicAndPartition + " failed", throwable);
        } finally {
          // notify to wake up producer's global lock
          synchronized (globalLock) {
            globalLock.notifyAll();
          }
        }
      } // while
    } // run

    private void putMessage(List<Message> messageList) {
      UserMessageResult userMessageResult = new UserMessageResult(
          messageList, partitionId);

      try {
        // when TalosProducer is disabled, we just fail the message and inform user;
        // but when TalosProducer is shutdown, we will send the left message.
        if (producer.isDisabled()) {
          throw new Throwable("The Topic: " + topicAndPartition.getTopicName() +
              " with resourceName: " + topicAndPartition.getTopicTalosResourceName() +
              " no longer exist. Please check the topic and reconstruct the" +
              " TalosProducer again");
        }

        long startPutMsgTime = System.currentTimeMillis();
        simpleProducer.doPut(messageList);
        producerMetrics.markPutMsgDuration(System.currentTimeMillis() - startPutMsgTime);
        // putMessage success callback
        userMessageResult.setSuccessful(true);
        messageCallbackExecutors.execute(
            new MessageCallbackTask(userMessageResult));
        if (LOG.isDebugEnabled()) {
          LOG.debug("put " + messageList.size() +
              " message success for partition: " + partitionId);
        }

        if (failedCounter.get() > 0) {
          failedCounter.set(0);
        }

      } catch (Throwable e) {
        producerMetrics.markPutMsgFailedTimes();
        failedCounter.incrementAndGet();
        LOG.error("Failed " + failedCounter.get() + " times to put " +
            messageList.size() + " messages for partition: " + partitionId, e);
        if (LOG.isDebugEnabled()) {
          for (Message message : messageList) {
            LOG.error(message.getSequenceNumber() + ": " +
                new String(message.getMessage()));
          }
        }
        // putMessage failed callback
        userMessageResult.setSuccessful(false).setCause(e);
        messageCallbackExecutors.execute(
            new MessageCallbackTask(userMessageResult));

        // delay when partitionNotServing
        if (Utils.isPartitionNotServing(e)) {
          LOG.warn("Partition: " + partitionId +
              " is not serving state, sleep a while for waiting it work.");
          try {
            Thread.sleep(talosProducerConfig.getWaitPartitionWorkingTime());
          } catch (InterruptedException e1) {
            e1.printStackTrace();
          }
        }

        // delay 10 * (n - 4) seconds when continuous failure >= 5 times
        if (Utils.isContinuousPutMsgFailed(failedCounter.get(), talosProducerConfig)) {
          LOG.warn("PutMessage failed " + failedCounter + " times for partition: " +
              partitionId + ", sleep a while for avoid infinitely retry.");
          long sleepTimeMS = Utils.getPutMsgFailedDelay(failedCounter.get(),
              talosProducerConfig);
          try {
            Thread.sleep(sleepTimeMS);
          } catch (InterruptedException e2) {
            LOG.error(e2.toString());
          }
        }

        // clear messageQueue and callback,
        if (Utils.isLongTimePutMsgFailed(failedCounter.get(), talosProducerConfig) &&
            isMsgQueueTooLarge(partitionMessageQueue.getCurMessageBytes())) {
          LOG.warn("PutMessage failed too many times for partition: " + partitionId +
              ", and already reach max put message limit, then will clear buffer.");

          // clear MessageQueue, and execute onError callback
          UserMessageResult bufferedUserMsgResult = new UserMessageResult(
              partitionMessageQueue.getAllMessageList(), partitionId);
          bufferedUserMsgResult.setSuccessful(false).setCause(e);
          messageCallbackExecutors.execute(
              new MessageCallbackTask(bufferedUserMsgResult));
        } // if
      } // catch
    }

  } // MessageWriter

  private static final Logger LOG = LoggerFactory.getLogger(PartitionSender.class);
  private int partitionId;
  // a static attribute of TalosProducer which will guarantee that
  // the requestId of all PartitionSender always be global unique
  private AtomicLong requestId;
  private String clientId;
  private TalosProducerConfig talosProducerConfig;
  private MessageService.Iface messageClient;
  private UserMessageCallback userMessageCallback;

  private TopicAndPartition topicAndPartition;
  private PartitionMessageQueue partitionMessageQueue;
  private ProducerMetrics producerMetrics;
  private ScheduledExecutorService singleExecutor;
  private Future messageWriterFuture;
  private ExecutorService messageCallbackExecutors;

  private final Object globalLock;
  private TalosProducer producer;

  public PartitionSender(int partitionId, String topicName,
      TopicTalosResourceName topicTalosResourceName, AtomicLong requestId,
      String clientId, TalosProducerConfig talosProducerConfig,
      MessageService.Iface messageClient, UserMessageCallback userMessageCallback,
      ExecutorService messageCallbackExecutors,
      Object globalLock, TalosProducer producer) {
    this.partitionId = partitionId;
    this.requestId = requestId;
    this.clientId = clientId;
    this.talosProducerConfig = talosProducerConfig;
    this.messageClient = messageClient;
    this.userMessageCallback = userMessageCallback;
    this.messageCallbackExecutors = messageCallbackExecutors;
    this.globalLock = globalLock;
    this.producer = producer;

    topicAndPartition = new TopicAndPartition(topicName,
        topicTalosResourceName, partitionId);
    partitionMessageQueue = new PartitionMessageQueue(talosProducerConfig,
        partitionId, producer);
    producerMetrics = new ProducerMetrics();
    singleExecutor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory(
        "talos-producer-" + topicName + ":" + partitionId));
    messageWriterFuture = singleExecutor.submit(new MessageWriter());
  }

  public void shutdown() {
    // notify PartitionMessageQueue::getMessageList return;
    addMessage(new ArrayList<UserMessage>());

    singleExecutor.shutdown();
    while (true) {
      try {
        if (singleExecutor.awaitTermination(100, TimeUnit.MILLISECONDS)) {
          break;
        }
      } catch (InterruptedException e) {

      }
    }
    LOG.info("PartitionSender for partition: " + partitionId + " finish stop");
  }

  public void addMessage(List<UserMessage> userMessageList) {
    partitionMessageQueue.addMessage(userMessageList);
    if (LOG.isDebugEnabled()) {
      LOG.debug("add " + userMessageList.size() +
          " messages to partition: " + partitionId);
    }
  }

  private boolean isMsgQueueTooLarge(int messageBytes) {
    return messageBytes >= talosProducerConfig.getClearMsgQueueBytesThreshold();
  }

  public JsonArray getFalconData() {
    return producerMetrics.toJsonData();
  }

  public class ProducerMetrics {
    private String falconEndpoint;
    private long putMsgDuration;
    private long maxPutMsgDuration;
    private long minPutMsgDuration;
    private int putMsgTimes;
    private int putMsgFailedTimes;
    private Map<String, Number> producerMetricsMap;

    public ProducerMetrics() {
      initMetrics();
      this.falconEndpoint = talosProducerConfig.getProducerMetricFalconEndpoint() +
          topicAndPartition.getTopicName();
      this.producerMetricsMap = new LinkedHashMap<String, Number>();
    }

    public void initMetrics() {
      this.putMsgDuration = 0;
      this.maxPutMsgDuration = 0;
      this.minPutMsgDuration = 0;
      this.putMsgTimes = 0;
      this.putMsgFailedTimes = 0;
    }

    public void markPutMsgDuration(long putMsgDuration) {
      if (putMsgDuration > maxPutMsgDuration) {
        this.maxPutMsgDuration = putMsgDuration;
      }

      if (minPutMsgDuration == 0 || putMsgDuration < minPutMsgDuration) {
        this.minPutMsgDuration = putMsgDuration;
      }

      this.putMsgDuration = putMsgDuration;
      this.putMsgTimes += 1;
    }

    public void markPutMsgFailedTimes() {
      this.putMsgFailedTimes += 1;
    }

    public JsonArray toJsonData() {
      updateMetricsMap();
      JsonArray jsonArray = new JsonArray();
      for (Map.Entry<String, Number> entry : producerMetricsMap.entrySet()) {
        JsonObject jsonObject = getBasicData();
        jsonObject.addProperty("metric", entry.getKey());
        jsonObject.addProperty("value", entry.getValue().doubleValue());
        jsonArray.add(jsonObject);
      }
      initMetrics();
      return jsonArray;
    }

    public void updateMetricsMap() {
      producerMetricsMap.put(Constants.PUT_MESSAGE_TIME, putMsgDuration);
      producerMetricsMap.put(Constants.MAX_PUT_MESSAGE_TIME, maxPutMsgDuration);
      producerMetricsMap.put(Constants.MIN_PUT_MESSAGE_TIME, minPutMsgDuration);
      producerMetricsMap.put(Constants.PUT_MESSAGE_TIMES, putMsgTimes / 60.0);
      producerMetricsMap.put(Constants.PUT_MESSAGE_FAILED_TIMES, putMsgFailedTimes/ 60.0);
    }

    private JsonObject getBasicData() {
      String tag = "clusterName=" + talosProducerConfig.getClusterName();
      tag += ",topicName=" + topicAndPartition.getTopicName();
      tag += ",partitionId=" + topicAndPartition.getPartitionId();
      tag += ",ip=" + talosProducerConfig.getClientIp();
      tag += ",type=" + talosProducerConfig.getAlertType();

      JsonObject basicData = new JsonObject();
      basicData.addProperty("endpoint", falconEndpoint);
      basicData.addProperty("timestamp", System.currentTimeMillis() / 1000);
      basicData.addProperty("step", talosProducerConfig.getMetricFalconStep() / 1000);
      basicData.addProperty("counterType", "GAUGE");
      basicData.addProperty("tags", tag);
      return basicData;
    }
  }
}
