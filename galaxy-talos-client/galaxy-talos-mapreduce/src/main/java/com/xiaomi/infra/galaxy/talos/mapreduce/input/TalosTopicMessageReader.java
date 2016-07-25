/**
 * Copyright 2016, Xiaomi.
 * All rights reserved.
 * Author: xiajun@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.mapreduce.input;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Properties;

import libthrift091.TException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.infra.galaxy.rpc.thrift.Credential;
import com.xiaomi.infra.galaxy.rpc.thrift.UserType;
import com.xiaomi.infra.galaxy.talos.client.TalosClientConfigKeys;
import com.xiaomi.infra.galaxy.talos.client.Utils;
import com.xiaomi.infra.galaxy.talos.consumer.SimpleConsumer;
import com.xiaomi.infra.galaxy.talos.consumer.TalosConsumerConfig;
import com.xiaomi.infra.galaxy.talos.mapreduce.input.config.TalosTopicInputConfiguration;
import com.xiaomi.infra.galaxy.talos.mapreduce.input.model.TalosTopicKeyWritable;
import com.xiaomi.infra.galaxy.talos.mapreduce.input.model.TalosTopicMessageWritable;
import com.xiaomi.infra.galaxy.talos.thrift.MessageAndOffset;
import com.xiaomi.infra.galaxy.talos.thrift.TopicAndPartition;
import com.xiaomi.infra.galaxy.talos.thrift.TopicTalosResourceName;

public class TalosTopicMessageReader extends RecordReader<TalosTopicKeyWritable, TalosTopicMessageWritable> {
  private static final Logger LOG = LoggerFactory.getLogger(TalosTopicMessageReader.class);

  private TalosTopicInputSplit inputSplit;
  private TalosTopicInputConfiguration configuration;

  private long nextMessageOffset;
  private LinkedList<MessageAndOffset> messageAndOffsetList;

  TalosConsumerConfig talosConsumerConfig;
  TopicAndPartition topicAndPartition;
  Credential credential;
  private SimpleConsumer simpleConsumer;

  public TalosTopicMessageReader() {
    System.out.print("construct TalosTopicMessageReader");
  }

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    this.inputSplit = (TalosTopicInputSplit)inputSplit;
    this.configuration = new TalosTopicInputConfiguration(taskAttemptContext.getConfiguration());

    nextMessageOffset = this.inputSplit.getStartMessageOffset();
    messageAndOffsetList = new LinkedList<MessageAndOffset>();

    // setup talosConsumerConfig;
    Properties properties = new Properties();
    properties.setProperty(TalosClientConfigKeys.GALAXY_TALOS_SERVICE_ENDPOINT,
        configuration.getTalosEndpoint());
    talosConsumerConfig = new TalosConsumerConfig(properties);

    // setup topicAndParition;
    topicAndPartition = new TopicAndPartition(
        Utils.getTopicNameByResourceName(configuration.getTopicResourceName()),
        new TopicTalosResourceName(configuration.getTopicResourceName()),
        this.inputSplit.getParititonId());

    // setup credential;
    credential = new Credential();
    credential.setSecretKeyId(configuration.getSecretId())
        .setSecretKey(configuration.getSecretKey())
        .setType(UserType.valueOf(configuration.getUserType()));

    // setup simpleConsumer;
    simpleConsumer = new SimpleConsumer(talosConsumerConfig, topicAndPartition, credential);
    System.out.println("create TalosTopicMessageReader success");
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    System.out.println("nextMessageOffset: " + nextMessageOffset + ", endMessageOffset: " + inputSplit.getEndMessageOffset());
    return nextMessageOffset < inputSplit.getEndMessageOffset();
  }

  @Override
  public TalosTopicKeyWritable getCurrentKey() throws IOException, InterruptedException {
    return new TalosTopicKeyWritable(inputSplit.getParititonId(), nextMessageOffset);
  }

  @Override
  public TalosTopicMessageWritable getCurrentValue() throws IOException, InterruptedException {
    MessageAndOffset messageAndOffset = messageAndOffsetList.pollFirst();

    // when there is no more message, we should fetch first;
    if (messageAndOffset == null) {
      fetchMessage();
      messageAndOffset = messageAndOffsetList.pollFirst();
    }

    // when getCurrentValue finished, we reset nextMessageOffset;
    nextMessageOffset = messageAndOffset.getMessageOffset() + 1;
    return new TalosTopicMessageWritable(messageAndOffset.getMessage());
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return (float)(nextMessageOffset - inputSplit.getStartMessageOffset()) /
        (float)(inputSplit.getEndMessageOffset() - inputSplit.getStartMessageOffset());
  }

  @Override
  public void close() throws IOException {

  }

  private void fetchMessage() throws IOException {
    int fetchMessageNumber = (int)(inputSplit.getEndMessageOffset() - nextMessageOffset);
    if (fetchMessageNumber > TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_MAX_FETCH_RECORDS_DEFAULT) {
      fetchMessageNumber = TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_MAX_FETCH_RECORDS_DEFAULT;
    }

    int retryTimes = 0;
    TException exception = null;
    while (retryTimes < configuration.getMaxRetrys()) {
      try {
        messageAndOffsetList.addAll(
            simpleConsumer.fetchMessage(nextMessageOffset, fetchMessageNumber));
        System.out.println("TopicAndPartition: " + topicAndPartition +
            " fetchMessage with startMessageOffset: " + nextMessageOffset +
            " success, messageNumber: " + messageAndOffsetList.size());
        return;
      } catch (TException e) {
        LOG.info("TopicAndPartition: " + topicAndPartition +
            " fetchMessage with startMessageOffset: " + nextMessageOffset + " failed", e);
        exception = e;
      }

      ++retryTimes;
    }

    throw new IOException("TopicAndPartition: " + topicAndPartition +
        " fetchMessage with startMessageOffset: " + nextMessageOffset + " failed", exception);
  }

}
