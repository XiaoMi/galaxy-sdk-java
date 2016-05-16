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

import com.xiaomi.infra.galaxy.rpc.thrift.Credential;
import com.xiaomi.infra.galaxy.talos.client.TalosClientConfigKeys;
import com.xiaomi.infra.galaxy.talos.client.TalosClientFactory;
import com.xiaomi.infra.galaxy.talos.client.Utils;
import com.xiaomi.infra.galaxy.talos.client.compression.Compression;
import com.xiaomi.infra.galaxy.talos.thrift.GetMessageRequest;
import com.xiaomi.infra.galaxy.talos.thrift.GetMessageResponse;
import com.xiaomi.infra.galaxy.talos.thrift.MessageAndOffset;
import com.xiaomi.infra.galaxy.talos.thrift.MessageOffset;
import com.xiaomi.infra.galaxy.talos.thrift.MessageService;
import com.xiaomi.infra.galaxy.talos.thrift.TopicAndPartition;
import com.xiaomi.infra.galaxy.talos.thrift.TopicTalosResourceName;

public class SimpleConsumer {
  private TopicAndPartition topicAndPartition;
  private MessageService.Iface messageClient;
  private TalosConsumerConfig consumerConfig;

  private static final AtomicLong requestId = new AtomicLong(1);
  private String simpleConsumerId;

  public SimpleConsumer(TalosConsumerConfig consumerConfig,
      TopicAndPartition topicAndPartition, MessageService.Iface messageClient,
      String consumerIdPrefix) {
    this.consumerConfig = consumerConfig;
    this.topicAndPartition = topicAndPartition;
    this.messageClient = messageClient;
    simpleConsumerId = Utils.generateClientId(consumerIdPrefix);
  }

  public SimpleConsumer(TalosConsumerConfig consumerConfig,
      TopicAndPartition topicAndPartition, MessageService.Iface messageClient) {
    this(consumerConfig, topicAndPartition, messageClient, "");
  }

  public SimpleConsumer(TalosConsumerConfig consumerConfig,
      TopicAndPartition topicAndPartition, Credential credential) {
    this(consumerConfig, topicAndPartition, new TalosClientFactory(
        consumerConfig, credential).newMessageClient());
  }

  public TopicTalosResourceName getTopicTalosResourceName() {
    return topicAndPartition.getTopicTalosResourceName();
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

    GetMessageResponse getMessageResponse = messageClient.getMessage(getMessageRequest);
    List<MessageAndOffset> messageAndOffsetList =
        Compression.decompress(getMessageResponse.getMessageBlocks());

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
}