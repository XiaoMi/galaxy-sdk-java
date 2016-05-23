/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: yongxing@xiaomi.com
 */

package com.xiaomi.infra.codelab.talos;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import libthrift091.TException;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.infra.galaxy.rpc.thrift.Credential;
import com.xiaomi.infra.galaxy.rpc.thrift.UserType;
import com.xiaomi.infra.galaxy.talos.admin.TalosAdmin;
import com.xiaomi.infra.galaxy.talos.client.SimpleTopicAbnormalCallback;
import com.xiaomi.infra.galaxy.talos.client.TalosClientConfig;
import com.xiaomi.infra.galaxy.talos.client.TalosClientConfigKeys;
import com.xiaomi.infra.galaxy.talos.consumer.MessageCheckpointer;
import com.xiaomi.infra.galaxy.talos.consumer.MessageProcessor;
import com.xiaomi.infra.galaxy.talos.consumer.MessageProcessorFactory;
import com.xiaomi.infra.galaxy.talos.consumer.TalosConsumer;
import com.xiaomi.infra.galaxy.talos.consumer.TalosConsumerConfig;
import com.xiaomi.infra.galaxy.talos.thrift.DescribeTopicRequest;
import com.xiaomi.infra.galaxy.talos.thrift.MessageAndOffset;
import com.xiaomi.infra.galaxy.talos.thrift.Topic;
import com.xiaomi.infra.galaxy.talos.thrift.TopicAndPartition;
import com.xiaomi.infra.galaxy.talos.thrift.TopicTalosResourceName;

public class TalosConsumerDemo {
  private static final Logger LOG = LoggerFactory.getLogger(TalosConsumerDemo.class);

  // callback for consumer to process messages, that is, consuming logic
  private static class MyMessageProcessor implements MessageProcessor {
    @Override
    public void init(TopicAndPartition topicAndPartition, long messageOffset) {

    }

    @Override
    public void process(List<MessageAndOffset> messages, MessageCheckpointer messageCheckpointer) {
      long count = successGetNumber.addAndGet(messages.size());
      if (messages.size() > 0) {
        LOG.info("Consuming total data so far: " + count +
            " and one message content: " +
            new String(messages.get(0).getMessage().getMessage()));
      }
    }

    @Override
    public void shutdown(MessageCheckpointer messageCheckpointer) {

    }
  }

  // using for thread-safe when processing different partition data
  private static class MyMessageProcessorFactory implements MessageProcessorFactory {
    @Override
    public MessageProcessor createProcessor() {
      return new MyMessageProcessor();
    }
  }

  private static final String talosServiceURI = "$talosServiceURI";
  private static final String appKeyId = "$your_appKey";
  private static final String appKeySecret = "$your_appSecret";
  private static final String topicName = "testTopic";
  private static final AtomicLong successGetNumber = new AtomicLong(0);

  private static final String clientPrefix = "departmentName-";
  private static final String consumerGroup = "groupName";

  private TalosConsumerConfig consumerConfig;
  private Credential credential;
  private TalosAdmin talosAdmin;
  private TalosConsumer talosConsumer;
  private TopicTalosResourceName topicTalosResourceName;

  public TalosConsumerDemo() throws TException {
    // init client config
    Configuration configuration = new Configuration();
    configuration.set(TalosClientConfigKeys.GALAXY_TALOS_SECURE_SERVICE_ENDPOINT,
        talosServiceURI);
    TalosClientConfig clientConfig = new TalosClientConfig(configuration);
    consumerConfig = new TalosConsumerConfig(configuration);

    // credential
    credential = new Credential();
    credential.setSecretKeyId(appKeyId).setSecretKey(appKeySecret)
        .setType(UserType.APP_SECRET);

    // get topic info
    talosAdmin = new TalosAdmin(clientConfig, credential);
    getTopicInfo();
  }

  private void getTopicInfo() throws TException {
    Topic topic = talosAdmin.describeTopic(new DescribeTopicRequest(topicName));
    topicTalosResourceName = topic.getTopicInfo().getTopicTalosResourceName();
  }

  public void start() throws TException {
    talosConsumer = new TalosConsumer(consumerGroup, consumerConfig,
        credential, topicTalosResourceName, new MyMessageProcessorFactory(),
        clientPrefix, new SimpleTopicAbnormalCallback());
  }

  public static void main(String[] args) throws Exception {
    TalosConsumerDemo consumerDemo = new TalosConsumerDemo();
    consumerDemo.start();
  }
}
