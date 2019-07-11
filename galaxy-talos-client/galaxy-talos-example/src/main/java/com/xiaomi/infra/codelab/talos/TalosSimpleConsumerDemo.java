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
import com.xiaomi.infra.galaxy.talos.consumer.SimpleConsumer;
import com.xiaomi.infra.galaxy.talos.consumer.TalosConsumerConfig;
import com.xiaomi.infra.galaxy.talos.thrift.MessageAndOffset;

public class TalosSimpleConsumerDemo {
  private static final Logger LOG = LoggerFactory.getLogger(TalosSimpleConsumerDemo.class);

  private static final String propertyFileName = "$your_propertyFile";
  private static final String accessKey = "$your_team_accessKey";
  private static final String accessSecret = "$your_team_accessSecret";
  private static final String topicName = "testTopic";
  private static final int partitionId = 0;

  private TalosConsumerConfig consumerConfig;
  private Credential credential;

  private static SimpleConsumer simpleConsumer;
  private long finishedOffset;
  private int maxFetchNum;

  public TalosSimpleConsumerDemo() {
    // init client config by put $your_propertyFile in your classpath
    // with the content of:
    /*
      galaxy.talos.service.endpoint=$talosServiceURI
    */
    consumerConfig = new TalosConsumerConfig(propertyFileName);

    // credential
    credential = new Credential();
    credential.setSecretKeyId(accessKey).setSecretKey(accessSecret)
        .setType(UserType.DEV_XIAOMI);
  }

  public void start() throws TException {
    // init consumer
    simpleConsumer = new SimpleConsumer(consumerConfig, topicName, partitionId, credential);
    finishedOffset = 0;
    maxFetchNum = 10;

    List<MessageAndOffset> messageList = new ArrayList<MessageAndOffset>();
    // a toy demo for putting messages to Talos server continuously
    // by using a infinite loop
    while (true) {
      try {
        messageList = simpleConsumer.fetchMessage(finishedOffset + 1, maxFetchNum);
        try {
          Thread.sleep(10);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        if (messageList.size() == 0) {continue;}
        finishedOffset = messageList.get(messageList.size() - 1).getMessageOffset();
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
