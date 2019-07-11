/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: yongxing@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.consumer;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Preconditions;
import libthrift091.TException;
import libthrift091.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.infra.galaxy.rpc.thrift.Credential;
import com.xiaomi.infra.galaxy.talos.client.ScheduleInfoCache;
import com.xiaomi.infra.galaxy.talos.client.TalosClientConfigKeys;
import com.xiaomi.infra.galaxy.talos.client.TalosClientFactory;
import com.xiaomi.infra.galaxy.talos.client.Utils;
import com.xiaomi.infra.galaxy.talos.client.compression.Compression;
import com.xiaomi.infra.galaxy.talos.thrift.GalaxyTalosException;
import com.xiaomi.infra.galaxy.talos.thrift.GetDescribeInfoRequest;
import com.xiaomi.infra.galaxy.talos.thrift.GetDescribeInfoResponse;
import com.xiaomi.infra.galaxy.talos.thrift.GetMessageRequest;
import com.xiaomi.infra.galaxy.talos.thrift.GetMessageResponse;
import com.xiaomi.infra.galaxy.talos.thrift.MessageAndOffset;
import com.xiaomi.infra.galaxy.talos.thrift.MessageOffset;
import com.xiaomi.infra.galaxy.talos.thrift.MessageService;
import com.xiaomi.infra.galaxy.talos.thrift.TopicAndPartition;
import com.xiaomi.infra.galaxy.talos.thrift.TopicService;
import com.xiaomi.infra.galaxy.talos.thrift.TopicTalosResourceName;

public class SimpleConsumer {
  private static final Logger LOG = LoggerFactory.getLogger(SimpleConsumer.class);

  private TopicAndPartition topicAndPartition;
  private MessageService.Iface messageClient;
  private TalosConsumerConfig consumerConfig;
  private ScheduleInfoCache scheduleInfoCache;

  private static final AtomicLong requestId = new AtomicLong(1);
  private String simpleConsumerId;

  public SimpleConsumer(TalosConsumerConfig consumerConfig,
      TopicAndPartition topicAndPartition, MessageService.Iface messageClient,
      String consumerIdPrefix) {
    this(consumerConfig, topicAndPartition, null,
        messageClient, consumerIdPrefix);
  }

  public SimpleConsumer(TalosConsumerConfig consumerConfig,
      TopicAndPartition topicAndPartition, MessageService.Iface messageClient) {
    // for TalosConsumer
    this(consumerConfig, topicAndPartition, messageClient, "");
  }

  public SimpleConsumer(TalosConsumerConfig consumerConfig, String topicName,
      int partitionId, Credential credential) throws GalaxyTalosException, TException{
    this(consumerConfig, topicName, partitionId, new TalosClientFactory(
        consumerConfig, credential), "");
  }

  @Deprecated
  public SimpleConsumer(TalosConsumerConfig consumerConfig,
      TopicAndPartition topicAndPartition, Credential credential) {
    this(consumerConfig, topicAndPartition, new TalosClientFactory(
        consumerConfig, credential));
  }

  public SimpleConsumer(TalosConsumerConfig consumerConfig,
      String topicName, int partitionId, TalosClientFactory talosClientFactory,
      String consumerIdPrefix) throws GalaxyTalosException, TException{
    Utils.checkTopicName(topicName);
    getTopicInfo(talosClientFactory.newTopicClient(), topicName, partitionId);
    this.consumerConfig = consumerConfig;
    this.messageClient = talosClientFactory.newMessageClient();
    simpleConsumerId = Utils.generateClientId(consumerIdPrefix);
    this.scheduleInfoCache = ScheduleInfoCache.getScheduleInfoCache(this.topicAndPartition.
        topicTalosResourceName, consumerConfig, this.messageClient, talosClientFactory);
  }

  public SimpleConsumer(TalosConsumerConfig consumerConfig,
      TopicAndPartition topicAndPartition, TalosClientFactory talosClientFactory,
      MessageService.Iface messageClient, String consumerIdPrefix) {
    Utils.checkTopicAndPartition(topicAndPartition);
    this.consumerConfig = consumerConfig;
    this.topicAndPartition = topicAndPartition;
    this.messageClient = messageClient;
    simpleConsumerId = Utils.generateClientId(consumerIdPrefix);
    this.scheduleInfoCache = ScheduleInfoCache.getScheduleInfoCache(topicAndPartition.
        topicTalosResourceName, consumerConfig, messageClient, talosClientFactory);
  }

  public SimpleConsumer(TalosConsumerConfig consumerConfig,
      TopicAndPartition topicAndPartition, TalosClientFactory talosClientFactory) {
    this(consumerConfig, topicAndPartition, talosClientFactory,
        talosClientFactory.newMessageClient(), "");
  }

  // for test
  public SimpleConsumer(TalosConsumerConfig consumerConfig, String topicName,
      int partitionId, MessageService.Iface messageClient,
      TopicService.Iface topicClient,
      ScheduleInfoCache scheduleInfoCacheMock) throws GalaxyTalosException, TException {
    Utils.checkTopicName(topicName);
    getTopicInfo(topicClient, topicName, partitionId);
    this.consumerConfig = consumerConfig;
    this.messageClient = messageClient;
    simpleConsumerId = Utils.generateClientId("");
    this.scheduleInfoCache = scheduleInfoCacheMock;
  }

  private void getTopicInfo(TopicService.Iface topicClient, String topicName,
      int partitionId) throws GalaxyTalosException, TException{
    GetDescribeInfoResponse response = topicClient.getDescribeInfo(
        new GetDescribeInfoRequest(topicName));
    this.topicAndPartition = new TopicAndPartition(topicName,
        response.getTopicTalosResourceName(), partitionId);
  }

  public TopicTalosResourceName getTopicTalosResourceName() {
    return topicAndPartition.getTopicTalosResourceName();
  }

  public void setSimpleConsumerId(String simpleConsumerId) {
    this.simpleConsumerId = simpleConsumerId;
  }

  public String getSimpleConsumerId() {
    return simpleConsumerId;
  }

  public int getPartitionId() {
    return topicAndPartition.getPartitionId();
  }

  public TopicAndPartition getTopicAndPartition() {
    return topicAndPartition;
  }

  public List<MessageAndOffset> fetchMessage(long startOffset) throws TException, IOException {
    return fetchMessage(startOffset, consumerConfig.getMaxFetchRecords());
  }

  public List<MessageAndOffset> fetchMessage(long startOffset,
      int maxFetchedNumber) throws TException, IOException {
    Utils.checkStartOffsetValidity(startOffset);
    Utils.checkParameterRange(
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_MAX_FETCH_RECORDS,
        maxFetchedNumber,
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_MAX_FETCH_RECORDS_MINIMUM,
        TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_MAX_FETCH_RECORDS_MAXIMUM);

    String requestSequenceId = Utils.generateRequestSequenceId(
        simpleConsumerId, requestId);
    // limit the default max fetch bytes 2M
    GetMessageRequest getMessageRequest = new GetMessageRequest(topicAndPartition,
        startOffset, requestSequenceId)
        .setMaxGetMessageNumber(maxFetchedNumber)
        .setMaxGetMessageBytes(TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_MAX_FETCH_BYTES_DEFAULT);
    int clientTimeout = consumerConfig.getClientTimeout();
    getMessageRequest.setTimeoutTimestamp(System.currentTimeMillis() + clientTimeout);

    GetMessageResponse getMessageResponse = new GetMessageResponse();
    try {
      getMessageResponse = scheduleInfoCache.getOrCreateMessageClient(topicAndPartition)
          .getMessage(getMessageRequest);
    } catch(TTransportException tTransportException){
      if (scheduleInfoCache != null && scheduleInfoCache.getIsAutoLocation()){
        LOG.warn("can't connect to the host directly, refresh scheduleInfo and retry using url. "
            + "The exception message is :" + tTransportException.getMessage() +
            ". Ignore this if not frequently.");
        scheduleInfoCache.updatescheduleInfoCache();
        getMessageRequest.setTimeoutTimestamp(System.currentTimeMillis() + clientTimeout);
        getMessageResponse = messageClient.getMessage(getMessageRequest);
      } else {
        throw tTransportException;
      }
    }

    //update scheduleInfocache when request have been transfered and talos auto location was set up
    if (getMessageResponse.isSetIsTransfer() && getMessageResponse.isIsTransfer() && scheduleInfoCache != null
        && scheduleInfoCache.getIsAutoLocation()) {
      LOG.info("request has been transfered when talos auto location set up, refresh scheduleInfo");
      scheduleInfoCache.updatescheduleInfoCache();
    }

    List<MessageAndOffset> messageAndOffsetList =
        Compression.decompress(getMessageResponse.getMessageBlocks(),
            getMessageResponse.getUnHandledMessageNumber());

    if (messageAndOffsetList.size() <= 0) {
      return messageAndOffsetList;
    }

    long actualStartOffset = messageAndOffsetList.get(0).getMessageOffset();

    if (messageAndOffsetList.get(0).getMessageOffset() == startOffset ||
        startOffset == MessageOffset.START_OFFSET.getValue() ||
        startOffset == MessageOffset.LATEST_OFFSET.getValue()) {
      return messageAndOffsetList;
    } else {
      int start = (int)(startOffset - actualStartOffset);
      Preconditions.checkArgument(start > 0, "Unexpected subList start index: " + start);
      int end = messageAndOffsetList.size();
      return messageAndOffsetList.subList(start, end);
    }
  }

  public void shutDown() {
    //onec called, all request of this topic in the process cannot auto location
    scheduleInfoCache.shutDown(topicAndPartition.topicTalosResourceName);
  }
}