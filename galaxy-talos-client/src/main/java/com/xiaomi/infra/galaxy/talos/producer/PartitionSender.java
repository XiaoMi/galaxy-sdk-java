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
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.infra.galaxy.talos.client.Utils;
import com.xiaomi.infra.galaxy.talos.client.compression.Compression;
import com.xiaomi.infra.galaxy.talos.thrift.Message;
import com.xiaomi.infra.galaxy.talos.thrift.MessageBlock;
import com.xiaomi.infra.galaxy.talos.thrift.MessageService;
import com.xiaomi.infra.galaxy.talos.thrift.PutMessageRequest;
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

    @Override
    public void run() {
      while (true) {
        try {
          // getUserMessageList will block unless return value not null
          List<UserMessage> userMessageList =
              partitionMessageQueue.getUserMessageList();
          putMessage(userMessageList);

          // notify to wake up producer's global lock
          synchronized (globalLock) {
            globalLock.notifyAll();
          }
        } catch (Throwable throwable) {
          LOG.error("PutMessageTask for topicAndPartition: " +
              topicAndPartition + " error: " + throwable.toString());
        }
      } // while
    } // run

    private void putMessage(List<UserMessage> userMessageList) {
      String requestSequenceId = Utils.generateRequestSequenceId(clientId, requestId);
      List<Message> messageList = new ArrayList<Message>(userMessageList.size());
      for (UserMessage userMessage : userMessageList) {
        messageList.add(userMessage.getMessage());
      }

      UserMessageResult userMessageResult = new UserMessageResult(
          messageList, partitionId);

      try {
        MessageBlock messageBlock = Compression.compress(messageList, talosProducerConfig.getCompressionType());
        List<MessageBlock> messageBlockList = new ArrayList<MessageBlock>(1);
        messageBlockList.add(messageBlock);

        PutMessageRequest putMessageRequest = new PutMessageRequest(
            topicAndPartition, messageBlockList, messageList.size(), requestSequenceId);

        messageClient.putMessage(putMessageRequest);
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
            " messages for partition: " + partitionId + " by: " + e.toString());
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
    topicAndPartition = new TopicAndPartition(topicName,
        topicTalosResourceName, partitionId);
    partitionMessageQueue = new PartitionMessageQueue(talosProducerConfig,
        partitionId, producer);
    singleExecutor = Executors.newSingleThreadScheduledExecutor();
    messageWriterFuture = singleExecutor.submit(new MessageWriter());
  }

  public void cancel(boolean interrupt) {
    messageWriterFuture.cancel(interrupt);
    singleExecutor.shutdownNow();
  }

  public void addMessage(List<UserMessage> userMessageList) {
    partitionMessageQueue.addMessage(userMessageList);
    if (LOG.isDebugEnabled()) {
      LOG.debug("add " + userMessageList.size() +
          " messages to partition: " + partitionId);
    }
  }
}
