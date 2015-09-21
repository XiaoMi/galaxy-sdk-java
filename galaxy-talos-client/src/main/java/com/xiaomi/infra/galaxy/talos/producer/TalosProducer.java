/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: yongxing@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.producer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;
import libthrift091.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.infra.galaxy.rpc.thrift.Credential;
import com.xiaomi.infra.galaxy.talos.admin.TalosAdmin;
import com.xiaomi.infra.galaxy.talos.client.Constants;
import com.xiaomi.infra.galaxy.talos.client.TalosClientFactory;
import com.xiaomi.infra.galaxy.talos.client.Utils;
import com.xiaomi.infra.galaxy.talos.thrift.Message;
import com.xiaomi.infra.galaxy.talos.thrift.MessageService;
import com.xiaomi.infra.galaxy.talos.thrift.PutMessageRequest;
import com.xiaomi.infra.galaxy.talos.thrift.PutMessageResponse;
import com.xiaomi.infra.galaxy.talos.thrift.Topic;
import com.xiaomi.infra.galaxy.talos.thrift.TopicAndPartition;
import com.xiaomi.infra.galaxy.talos.thrift.TopicTalosResourceName;

public class TalosProducer {

  private class CheckPartitionTask implements Runnable {
    @Override
    public void run() {
      Topic topic;
      try {
        topic = talosAdmin.describeTopic(topicName);
      } catch (Throwable throwable) {
        LOG.error("Exception in UpdatePartitionTask: " + throwable.toString());
        return;
      }

      if (!topicTalosResourceName.equals(
          topic.getTopicInfo().getTopicTalosResourceName())) {
        throw new RuntimeException("The topic: " +
            topicTalosResourceName.getTopicTalosResourceName() +
            " not exist. It might have been deleted");
      }

      int topicPartitionNum = topic.getTopicAttribute().getPartitionNumber();
      if (partitionNumber > topicPartitionNum) {
        // increase partitionQueue and scan thread (not allow decreasing)
        adjustPartitionMessageQueue(topicPartitionNum);
        // update partitionNumber
        setPartitionNumber(topicPartitionNum);
      }
    }
  }

  private class PutMessageTask implements Runnable {
    private PartitionMessageQueue partitionMessageQueue;
    private TopicAndPartition topicAndPartition;
    private int partitionId;

    private PutMessageTask(PartitionMessageQueue partitionMessageQueue,
        int partitionId) {
      this.partitionId = partitionId;
      this.partitionMessageQueue = partitionMessageQueue;
      topicAndPartition = new TopicAndPartition(topicName,
          topicTalosResourceName, partitionId);
    }

    @SuppressWarnings("unchecked")
    private void setFutureOnSuccess(List<MessageAndFuture> messageAndFutureList) {
      for (MessageAndFuture messageAndFuture : messageAndFutureList) {
        UserMessageResult userMessageResult = new UserMessageResult(
            partitionId, true);
        messageAndFuture.getFuture().set(userMessageResult);
      }
    }

    @SuppressWarnings("unchecked")
    private void setFutureOnFailed(List<MessageAndFuture> messageAndFutureList,
        TException e) {
      for (MessageAndFuture messageAndFuture : messageAndFutureList) {
        messageAndFuture.getFuture().setException(e);
      }
    }

    private void putMessage(List<MessageAndFuture> messageAndFutureList) {
      String requestSequenceId = generateRequestSequenceId();
      List<Message> messageList = new ArrayList<Message>(messageAndFutureList.size());
      for (MessageAndFuture messageAndFuture : messageAndFutureList) {
        messageList.add(messageAndFuture.getMessage());
      }
      PutMessageRequest putMessageRequest = new PutMessageRequest(
          topicAndPartition, messageList, requestSequenceId);
      PutMessageResponse putMessageResponse;

      try {
        putMessageResponse = messageClient.putMessage(putMessageRequest);
        setFutureOnSuccess(messageAndFutureList); // set future for success
      } catch (TException e) {
        LOG.error("Failed to putMessage in partition " + partitionId + ": ");
        for (Message message : messageList) {
          LOG.error(message.getSequenceNumber() + ": " +
              new String(message.getMessage()));
        }
        // set future for failed
        setFutureOnFailed(messageAndFutureList, e);
      }
    }

    @Override
    public void run() {
      while (true) {
        try {
          List<MessageAndFuture> messageAndFutureList =
              partitionMessageQueue.getMessageAndFutureList();

          if (messageAndFutureList != null) {
            putMessage(messageAndFutureList);
          } else {
            try {
              Thread.sleep(talosProducerConfig.getScanPartitionQueueInterval());
            } catch (InterruptedException e) {
              LOG.error("Sleep is interrupted in putMessage Task: " + e.toString());
            }
          } // else
        } catch (Throwable throwable) {
          LOG.error("PutMessageTask for topicAndPartition: " +
              topicAndPartition + " error: " + throwable.toString());
        }
      } // while
    } // run
  }

  private static final Logger LOG = LoggerFactory.getLogger(TalosProducer.class);
  private final int partitionKeyMinLen = Constants.TALOS_PARTITION_KEY_LENGTH_MINIMAL;
  private final int partitionKeyMaxLen = Constants.TALOS_PARTITION_KEY_LENGTH_MAXIMAL;
  private final AtomicLong requestId = new AtomicLong(1);

  private final Map<Integer, PartitionMessageQueue> outgoingMessageMap =
      new ConcurrentHashMap<Integer, PartitionMessageQueue>();
  private TalosProducerConfig talosProducerConfig;
  private ScheduledExecutorService scheduledExecutor;
  private TalosClientFactory talosClientFactory;
  private MessageService.Iface messageClient;
  private TalosAdmin talosAdmin;
  private TopicTalosResourceName topicTalosResourceName;
  private String topicName;
  private Partitioner partitioner;
  private String clientId;
  private int partitionNumber;

  public TalosProducer(TalosProducerConfig producerConfig,
      TopicTalosResourceName topicTalosResourceName) throws TException {
    new TalosProducer(producerConfig, new Credential(),
        topicTalosResourceName, new SimplePartitioner());
  }

  public TalosProducer(TalosProducerConfig producerConfig, Credential credential,
      TopicTalosResourceName topicTalosResourceName) throws TException {
    new TalosProducer(producerConfig, credential,
        topicTalosResourceName, new SimplePartitioner());
  }

  public TalosProducer(TalosProducerConfig producerConfig, Credential credential,
      TopicTalosResourceName topicTalosResourceName, Partitioner partitioner)
      throws TException {
    this.partitioner = partitioner;
    talosProducerConfig = producerConfig;
    clientId = generateClientId();
    talosClientFactory = new TalosClientFactory(talosProducerConfig, credential);
    checkAndGetTopicInfo(topicTalosResourceName);

    // Note: all the params of newMessageClient got by producerConfig
    // if user want set a new serviceEndpoint, just set in producerConfig
    messageClient = talosClientFactory.newMessageClient();
    scheduledExecutor = Executors.newScheduledThreadPool(
        talosProducerConfig.getThreadPoolsize());
    initPartitionMessageQueue();
    initCheckPartitionTask();
    LOG.info("Init a producer for topic: " +
        topicTalosResourceName.getTopicTalosResourceName() +
        ", partitions: " + partitionNumber);
  }

  private void checkAndGetTopicInfo(TopicTalosResourceName topicTalosResourceName)
      throws TException {
    topicName = Utils.getTopicNameByResourceName(
        topicTalosResourceName.getTopicTalosResourceName());
    talosAdmin = new TalosAdmin(talosClientFactory);
    Topic topic = talosAdmin.describeTopic(topicName);

    if (!topicTalosResourceName.equals(
        topic.getTopicInfo().getTopicTalosResourceName())) {
      throw new IllegalArgumentException("The topic: " +
          topicTalosResourceName.getTopicTalosResourceName() + " not found");
    }
    partitionNumber = topic.getTopicAttribute().getPartitionNumber();
    this.topicTalosResourceName = topicTalosResourceName;
  }

  private void createPartitionMessageQueue(int partitionId) {
    PartitionMessageQueue partitionMessageQueue =
        new PartitionMessageQueue(talosProducerConfig);
    outgoingMessageMap.put(partitionId, partitionMessageQueue);

    // schedule a executor thread to call putMessage continuously
    // when add a new partitionMessageQueue
    // scan msgQueue after 10 milli secs by default
    scheduledExecutor.schedule(
        new PutMessageTask(partitionMessageQueue, partitionId),
        talosProducerConfig.getScanPartitionQueueInterval(),
        TimeUnit.MILLISECONDS);
  }

  private void initPartitionMessageQueue() {
    for (int partitionId = 0; partitionId < partitionNumber; ++partitionId) {
      createPartitionMessageQueue(partitionId);
    }
  }

  private void adjustPartitionMessageQueue(int newPartitionNum) {
    // Note: we do not allow and process 'newPartitionNum < partitionNumber'
    if (newPartitionNum > partitionNumber) {
      for (int partitionId = partitionNumber; partitionId < newPartitionNum;
           ++partitionId) {
        createPartitionMessageQueue(partitionId);
      }
    }
  }

  private void initCheckPartitionTask() {
    // check and update partition number every 3 minutes by default
    scheduledExecutor.scheduleAtFixedRate(new CheckPartitionTask(),
        talosProducerConfig.getCheckPartitionInterval(),
        talosProducerConfig.getCheckPartitionInterval(), TimeUnit.MILLISECONDS);
  }

  private int getPartitionId(String partitionKey) {
    return partitioner.partition(partitionKey, partitionNumber);
  }

  private String generateClientId() {
    return System.currentTimeMillis() + UUID.randomUUID().toString().substring(0, 8);
  }

  private String generatePartitionKey() {
    return UUID.randomUUID().toString();
  }

  private String generateRequestSequenceId() {
    return clientId + Constants.TALOS_IDENTIFIER_DELIMITER +
        requestId.getAndIncrement();
  }

  public synchronized void setPartitionNumber(int partitionNumber) {
    this.partitionNumber = partitionNumber;
  }

  public ListenableFuture<UserMessageResult> addUserMessage(ByteBuffer data) {
    return addUserMessage(generatePartitionKey(), null, data);
  }

  public ListenableFuture<UserMessageResult> addUserMessage(String partitionKey,
      String sequenceNumber, ByteBuffer data) {
    // check arguments
    checkUserMessageValidity(partitionKey, data);

    // construct message
    Message message = new Message(data)
        .setPartitionKey(partitionKey)
        .setSequenceNumber(sequenceNumber);
    MessageAndFuture messageAndFuture = new MessageAndFuture(message);

    // dispatch message by partitionId
    int partitionId = getPartitionId(partitionKey);
    Preconditions.checkArgument(outgoingMessageMap.containsKey(partitionId));
    outgoingMessageMap.get(partitionId).addMessage(messageAndFuture);

    return messageAndFuture.getFuture();
  }

  private void checkUserMessageValidity(String partitionKey, ByteBuffer data) {
    if (partitionKey.length() < partitionKeyMinLen ||
        partitionKey.length() > partitionKeyMaxLen) {
      throw new IllegalArgumentException("Invalid partition key which length " +
          "must be at least " + partitionKeyMinLen + " and at most " +
          partitionKeyMinLen + ", got " + partitionKey.length());
    }

    if (data != null &&
        data.remaining() > Constants.TALOS_SINGLE_MESSAGE_BYTES_MAXIMAL) {
      throw new IllegalArgumentException("Data must be less than or equal to " +
          Constants.TALOS_SINGLE_MESSAGE_BYTES_MAXIMAL + " bytes, got bytes: " +
          data.remaining());
    }
  }
}
