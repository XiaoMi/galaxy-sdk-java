/**
 * Copyright 2018, Xiaomi. All rights reserved. Author: zhangqian8@xiaomi.com
 */
package com.xiaomi.infra.codelab.talos;

import java.util.ArrayList;
import java.util.List;

import java.io.IOException;
import libthrift091.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.infra.galaxy.rpc.thrift.Credential;
import com.xiaomi.infra.galaxy.rpc.thrift.UserType;
import com.xiaomi.infra.galaxy.talos.admin.TalosAdmin;
import com.xiaomi.infra.galaxy.talos.client.TalosClientConfig;
import com.xiaomi.infra.galaxy.talos.consumer.SimpleConsumer;
import com.xiaomi.infra.galaxy.talos.consumer.TalosConsumerConfig;
import com.xiaomi.infra.galaxy.talos.thrift.DescribeTopicRequest;
import com.xiaomi.infra.galaxy.talos.thrift.MessageAndOffset;
import com.xiaomi.infra.galaxy.talos.thrift.Topic;
import com.xiaomi.infra.galaxy.talos.thrift.TopicAndPartition;
import com.xiaomi.infra.galaxy.talos.thrift.TopicTalosResourceName;

public class TalosSimpleConsumerDemo {
  private static final Logger LOG = LoggerFactory.getLogger(TalosSimpleConsumerDemo.class);

  private static final String propertyFileName = "$your_propertyFile";
  private static final String accessKey = "$your_team_accessKey";
  private static final String accessSecret = "$your_team_accessSecret";
  private static final String topicName = "testTopic";
  private static final int partitionId1 = 1;

  private TalosClientConfig clientConfig;
  private TalosConsumerConfig consumerConfig;
  private Credential credential;
  private TalosAdmin talosAdmin;

  private TopicTalosResourceName topicTalosResourceName;
  private static SimpleConsumer simpleConsumer;
  private long finishedOffset;
  private int maxFetchNum;

  public TalosSimpleConsumerDemo() throws Exception {
    // init client config by put $your_propertyFile in your classpath
    // with the content of:
    /*
      galaxy.talos.service.endpoint=$talosServiceURI
    */
    clientConfig = new TalosClientConfig(propertyFileName);
    consumerConfig = new TalosConsumerConfig(propertyFileName);

    // credential
    credential = new Credential();
    credential.setSecretKeyId(accessKey).setSecretKey(accessSecret)
        .setType(UserType.DEV_XIAOMI);

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
    TopicAndPartition topicAndPartition1 = new TopicAndPartition(
        topicName, topicTalosResourceName, partitionId1);
    simpleConsumer = new SimpleConsumer(consumerConfig,
        topicAndPartition1, credential);
    finishedOffset = 0;
    maxFetchNum = 10;

    List<MessageAndOffset> messageList = new ArrayList<MessageAndOffset>();
    // a toy demo for putting messages to Talos server continuously
    // by using a infinite loop
    while (true) {
      try {
        messageList = simpleConsumer.fetchMessage(finishedOffset + 1, maxFetchNum);
        finishedOffset = messageList.get(messageList.size() - 1).getMessageOffset();
        try {
          Thread.sleep(10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        LOG.info("success get message count: " + messageList.size());
      } catch (IOException e) {
        LOG.warn("get message failed, try again");
      }
    }
  }

  public static void main(String[] args) throws Exception {
    TalosSimpleConsumerDemo simpleConsumerDemo = new TalosSimpleConsumerDemo();
    // add message list to producer continuously
    simpleConsumerDemo.start();
  }
}
