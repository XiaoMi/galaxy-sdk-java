/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: yongxing@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.producer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import libthrift091.TException;
import libthrift091.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.infra.galaxy.rpc.thrift.Credential;
import com.xiaomi.infra.galaxy.talos.client.ScheduleInfoCache;
import com.xiaomi.infra.galaxy.talos.client.TalosClientFactory;
import com.xiaomi.infra.galaxy.talos.client.Utils;
import com.xiaomi.infra.galaxy.talos.client.compression.Compression;
import com.xiaomi.infra.galaxy.talos.thrift.GalaxyTalosException;
import com.xiaomi.infra.galaxy.talos.thrift.GetDescribeInfoRequest;
import com.xiaomi.infra.galaxy.talos.thrift.GetDescribeInfoResponse;
import com.xiaomi.infra.galaxy.talos.thrift.Message;
import com.xiaomi.infra.galaxy.talos.thrift.MessageBlock;
import com.xiaomi.infra.galaxy.talos.thrift.MessageService;
import com.xiaomi.infra.galaxy.talos.thrift.MessageType;
import com.xiaomi.infra.galaxy.talos.thrift.PutMessageRequest;
import com.xiaomi.infra.galaxy.talos.thrift.PutMessageResponse;
import com.xiaomi.infra.galaxy.talos.thrift.TopicAndPartition;
import com.xiaomi.infra.galaxy.talos.thrift.TopicService;

public class SimpleProducer {
  private static final Logger LOG = LoggerFactory.getLogger(SimpleProducer.class);
  private TalosProducerConfig producerConfig;
  private TopicAndPartition topicAndPartition;
  private MessageService.Iface messageClient;
  private AtomicLong requestId;
  private String clientId;
  private ScheduleInfoCache scheduleInfoCache;

  public SimpleProducer(TalosProducerConfig producerConfig,
      TopicAndPartition topicAndPartition, MessageService.Iface messageClient,
      String clientId, AtomicLong requestId) {
    // for TalosProducer, user don't use
    this(producerConfig, topicAndPartition, null, messageClient,
        clientId, requestId);
  }

  public SimpleProducer(TalosProducerConfig producerConfig,
      TopicAndPartition topicAndPartition, MessageService.Iface messageClient,
      AtomicLong requestId) {
    // for TalosProducer, user don't use
    this(producerConfig, topicAndPartition, messageClient,
        Utils.generateClientId(SimpleProducer.class.getSimpleName()), requestId);
  }

  public SimpleProducer(TalosProducerConfig producerConfig,
      TopicAndPartition topicAndPartition, TalosClientFactory talosClientFactory,
      AtomicLong requestId) {
    this(producerConfig, topicAndPartition, talosClientFactory,
        talosClientFactory.newMessageClient(),
        Utils.generateClientId(SimpleProducer.class.getSimpleName()), requestId);
  }

  public SimpleProducer(TalosProducerConfig producerConfig, String topicName,
      int partitionId, Credential credential) throws GalaxyTalosException, TException{
    this(producerConfig, topicName, partitionId, new TalosClientFactory(producerConfig,
            credential), Utils.generateClientId(SimpleProducer.class.getSimpleName()),
        new AtomicLong(1));
  }

  // User need construct config, topicAndPartition, credential
  @Deprecated
  public SimpleProducer(TalosProducerConfig producerConfig,
      TopicAndPartition topicAndPartition, Credential credential) {
    this(producerConfig, topicAndPartition, new TalosClientFactory(
        producerConfig, credential), new AtomicLong(1));
  }

  public SimpleProducer(TalosProducerConfig producerConfig,
      String topicName, int partitionId, TalosClientFactory talosClientFactory,
      String clientId, AtomicLong requestId) throws GalaxyTalosException, TException{
    Utils.checkTopicName(topicName);
    getTopicInfo(talosClientFactory.newTopicClient(), topicName, partitionId);
    this.producerConfig = producerConfig;
    this.messageClient = talosClientFactory.newMessageClient();
    this.clientId = clientId;
    this.requestId = requestId;
    this.scheduleInfoCache = ScheduleInfoCache.getScheduleInfoCache(this.topicAndPartition.
        topicTalosResourceName, producerConfig, messageClient, talosClientFactory);
  }

  public SimpleProducer(TalosProducerConfig producerConfig,
      TopicAndPartition topicAndPartition, TalosClientFactory talosClientFactory,
      MessageService.Iface messageClient, String clientId, AtomicLong requestId) {
    Utils.checkTopicAndPartition(topicAndPartition);
    this.producerConfig = producerConfig;
    this.topicAndPartition = topicAndPartition;
    this.messageClient = messageClient;
    this.clientId = clientId;
    this.requestId = requestId;
    this.scheduleInfoCache = ScheduleInfoCache.getScheduleInfoCache(topicAndPartition.
        topicTalosResourceName, producerConfig, messageClient, talosClientFactory);
  }

  // for test
  public SimpleProducer(TalosProducerConfig producerConfig, String topicName,
      int partitionId, MessageService.Iface messageClientMock,
      TopicService.Iface topicClient, AtomicLong requestId,
      ScheduleInfoCache scheduleInfoCacheMock) throws GalaxyTalosException, TException{
    Utils.checkTopicName(topicName);
    getTopicInfo(topicClient, topicName, partitionId);
    this.producerConfig = producerConfig;
    this.messageClient = messageClientMock;
    this.clientId = Utils.generateClientId(SimpleProducer.class.getSimpleName());
    this.requestId = requestId;
    this.scheduleInfoCache = scheduleInfoCacheMock;
  }

  private void getTopicInfo(TopicService.Iface topicClient, String topicName,
      int partitionId) throws GalaxyTalosException, TException{
    GetDescribeInfoResponse response = topicClient.getDescribeInfo(
        new GetDescribeInfoRequest(topicName));
    this.topicAndPartition = new TopicAndPartition(topicName,
        response.getTopicTalosResourceName(), partitionId);
  }

  @Deprecated
  public boolean putMessage(List<Message> msgList) {
    if (msgList == null || msgList.size() == 0) {
      return true;
    }

    try {
      putMessageList(msgList);
      return true;
    } catch (Exception e) {
      LOG.error("putMessage error: " +
          ", please try to put again", e);
    }
    return false;
  }

  public void putMessageList(List<Message> msgList) throws IOException, TException {
    if (msgList == null || msgList.size() == 0) {
      return;
    }

    // check data validity
    for (Message message : msgList) {
      // set timestamp and messageType if not set;
      Utils.updateMessage(message, MessageType.BINARY);
    }

    // check data validity
    Utils.checkMessageValidity(msgList);

    doPut(msgList);
  }

  protected void doPut(List<Message> msgList) throws IOException, TException {
    // TODO: compressMessageList return List<MessageBlock> when MsgList is too large;
    MessageBlock messageBlock = compressMessageList(msgList);
    List<MessageBlock> messageBlockList = new ArrayList<MessageBlock>(1);
    messageBlockList.add(messageBlock);

    String requestSequenceId = Utils.generateRequestSequenceId(clientId, requestId);
    PutMessageRequest putMessageRequest = new PutMessageRequest(
        topicAndPartition, messageBlockList,
        msgList.size(), requestSequenceId);
    int clientTimeout = producerConfig.getClientTimeout();
    putMessageRequest.setTimeoutTimestamp(System.currentTimeMillis() + clientTimeout);
    PutMessageResponse putMessageResponse = new PutMessageResponse();
    try {
      putMessageResponse = scheduleInfoCache.getOrCreateMessageClient(topicAndPartition)
          .putMessage(putMessageRequest);
    } catch(TTransportException tTransportException){
      if (scheduleInfoCache != null && scheduleInfoCache.getIsAutoLocation()) {
        LOG.warn("can't connect to the host directly, refresh scheduleInfo and retry using url. "
            + "The exception message is :" + tTransportException.getMessage() +
            ". Ignore this if not frequently.");
        scheduleInfoCache.updatescheduleInfoCache();
        putMessageRequest.setTimeoutTimestamp(System.currentTimeMillis() + clientTimeout);
        putMessageResponse = messageClient.putMessage(putMessageRequest);
      } else {
        throw tTransportException;
      }
    }

    //update scheduleInfocache when request have been transfered and talos auto location was set up
    if (putMessageResponse.isSetIsTransfer() && putMessageResponse.isIsTransfer() && scheduleInfoCache != null
        && scheduleInfoCache.getIsAutoLocation()) {
      LOG.info("request has been transfered when talos auto location set up, refresh scheduleInfo");
      scheduleInfoCache.updatescheduleInfoCache();
    }
  }

  protected MessageBlock compressMessageList(List<Message> messageList) throws IOException {
    return Compression.compress(messageList,
        producerConfig.getCompressionType());
  }

  public void shutDown() {
    //onec called, all request of this topic in the process cannot auto location
    scheduleInfoCache.shutDown(topicAndPartition.topicTalosResourceName);
  }
}