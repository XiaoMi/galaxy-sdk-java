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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.infra.galaxy.rpc.thrift.Credential;
import com.xiaomi.infra.galaxy.talos.client.TalosClientFactory;
import com.xiaomi.infra.galaxy.talos.client.Utils;
import com.xiaomi.infra.galaxy.talos.client.compression.Compression;
import com.xiaomi.infra.galaxy.talos.thrift.Message;
import com.xiaomi.infra.galaxy.talos.thrift.MessageBlock;
import com.xiaomi.infra.galaxy.talos.thrift.MessageService;
import com.xiaomi.infra.galaxy.talos.thrift.PutMessageRequest;
import com.xiaomi.infra.galaxy.talos.thrift.TopicAndPartition;

public class SimpleProducer {
  private static final Logger LOG = LoggerFactory.getLogger(SimpleProducer.class);
  private TalosProducerConfig producerConfig;
  private TopicAndPartition topicAndPartition;
  private MessageService.Iface messageClient;
  private AtomicLong requestId;
  private String clientId;

  public SimpleProducer(TalosProducerConfig producerConfig,
      TopicAndPartition topicAndPartition, MessageService.Iface messageClient,
      String clientId, AtomicLong requestId) {
    Utils.checkTopicAndPartition(topicAndPartition);
    this.producerConfig = producerConfig;
    this.topicAndPartition = topicAndPartition;
    this.messageClient = messageClient;
    this.clientId = clientId;
    this.requestId = requestId;
  }

  public SimpleProducer(TalosProducerConfig producerConfig,
      TopicAndPartition topicAndPartition, MessageService.Iface messageClient,
      AtomicLong requestId) {
    this(producerConfig, topicAndPartition, messageClient,
        Utils.generateClientId(SimpleProducer.class.getSimpleName()), requestId);
  }

  // User need construct config, topicAndPartition, credential
  public SimpleProducer(TalosProducerConfig producerConfig,
      TopicAndPartition topicAndPartition, Credential credential) {
    this(producerConfig, topicAndPartition, new TalosClientFactory(
        producerConfig, credential).newMessageClient(), new AtomicLong(1));
  }

  public boolean putMessage(List<Message> msgList) {
    if (msgList == null || msgList.size() == 0) {
      return true;
    }

    // check data validity
    for (Message message : msgList) {
      Utils.checkMessageLenValidity(message.getMessage());
      Utils.checkMessageSequenceNumberValidity(message);
    }

    try {
      doPut(msgList);
      return true;
    } catch (Exception e) {
      LOG.error("putMessage error: " + e.toString() +
          ", please try to put again");
    }
    return false;
  }

  protected void doPut(List<Message> msgList) throws IOException, TException {
    MessageBlock messageBlock = Compression.compress(msgList,
        producerConfig.getCompressionType());
    List<MessageBlock> messageBlockList = new ArrayList<MessageBlock>(1);
    messageBlockList.add(messageBlock);

    String requestSequenceId = Utils.generateRequestSequenceId(clientId, requestId);
    PutMessageRequest putMessageRequest = new PutMessageRequest(
        topicAndPartition, messageBlockList,
        msgList.size(), requestSequenceId);
    messageClient.putMessage(putMessageRequest);
  }
}