/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: yongxing@xiaomi.com
 */

package com.xiaomi.infra.codelab.talos;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import libthrift091.TException;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.infra.galaxy.rpc.thrift.Credential;
import com.xiaomi.infra.galaxy.rpc.thrift.UserType;
import com.xiaomi.infra.galaxy.talos.admin.TalosAdmin;
import com.xiaomi.infra.galaxy.talos.client.TalosClientConfig;
import com.xiaomi.infra.galaxy.talos.client.TalosClientConfigKeys;
import com.xiaomi.infra.galaxy.talos.producer.SimpleProducer;
import com.xiaomi.infra.galaxy.talos.producer.TalosProducerConfig;
import com.xiaomi.infra.galaxy.talos.thrift.DescribeTopicRequest;
import com.xiaomi.infra.galaxy.talos.thrift.Message;
import com.xiaomi.infra.galaxy.talos.thrift.Topic;
import com.xiaomi.infra.galaxy.talos.thrift.TopicAndPartition;
import com.xiaomi.infra.galaxy.talos.thrift.TopicTalosResourceName;

public class TalosSimpleProducerDemo {
  private static final Logger LOG = LoggerFactory.getLogger(TalosSimpleProducerDemo.class);

  private static final String talosServiceURI = "https://cnbj0.talos.api.xiaomi.com";
  private static final String appKeyId = "$your_appKey";
  private static final String appKeySecret = "$your_appSecret";
  private static final String topicName = "testTopic";
  private static final int partitionId = 7;
  private static final AtomicLong successPutNumber = new AtomicLong(0);

  private TalosClientConfig clientConfig;
  private TalosProducerConfig producerConfig;
  private Credential credential;
  private TalosAdmin talosAdmin;

  private TopicTalosResourceName topicTalosResourceName;
  private static SimpleProducer simpleProducer;

  public TalosSimpleProducerDemo() throws Exception {
    // init client config
    Configuration configuration = new Configuration();
    configuration.set(TalosClientConfigKeys.GALAXY_TALOS_SECURE_SERVICE_ENDPOINT,
        talosServiceURI);
    clientConfig = new TalosClientConfig(configuration);
    producerConfig = new TalosProducerConfig(configuration);

    // credential
    credential = new Credential();
    credential.setSecretKeyId(appKeyId).setSecretKey(appKeySecret)
        .setType(UserType.APP_SECRET);

    // init admin and try to get or create topic info
    talosAdmin = new TalosAdmin(clientConfig, credential);
    getTopicInfo();
  }

  private void getTopicInfo() throws Exception {
    Topic topic = talosAdmin.describeTopic(new DescribeTopicRequest(topicName));
    topicTalosResourceName = topic.getTopicInfo().getTopicTalosResourceName();
  }

  public void start() throws TException {
    // init producer
    TopicAndPartition topicAndPartition = new TopicAndPartition(
        topicName, topicTalosResourceName, partitionId);
    simpleProducer = new SimpleProducer(producerConfig,
        topicAndPartition, credential);

    String messageStr = "test message: this message is a text string.";
    Message message = new Message(ByteBuffer.wrap(messageStr.getBytes()));
    List<Message> messageList = new ArrayList<Message>();
    messageList.add(message);
    // a toy demo for putting messages to Talos server continuously
    while (true) {
      boolean putState = simpleProducer.putMessage(messageList);
      while (!putState) {
        LOG.warn("put message failed, try again");
        putState = simpleProducer.putMessage(messageList);
      }
      LOG.info("success put message count: " + successPutNumber.getAndIncrement());
    }
  }

  public static void main(String[] args) throws Exception {
    TalosSimpleProducerDemo simpleProducerDemo = new TalosSimpleProducerDemo();
    // add message list to producer continuously
    simpleProducerDemo.start();
  }
}
