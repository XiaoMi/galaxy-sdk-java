/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: yongxing@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.producer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private MessageWriter() {
      simpleProducer = new SimpleProducer(talosProducerConfig,
          topicAndPartition, messageClient, clientId, requestId);
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

        simpleProducer.doPut(messageList);
        // putMessage success callback
        userMessageResult.setSuccessful(true);
        messageCallbackExecutors.execute(
            new MessageCallbackTask(userMessageResult));
        if (LOG.isDebugEnabled()) {
          LOG.debug("put " + messageList.size() +
              " message success for partition: " + partitionId);
        }
      } catch (Throwable e) {
        LOG.error("Failed to put " + messageList.size() +
            " messages for partition: " + partitionId, e);
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
}
