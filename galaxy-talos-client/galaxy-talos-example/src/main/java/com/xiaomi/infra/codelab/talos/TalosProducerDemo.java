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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.infra.galaxy.rpc.thrift.Credential;
import com.xiaomi.infra.galaxy.rpc.thrift.UserType;
import com.xiaomi.infra.galaxy.talos.admin.TalosAdmin;
import com.xiaomi.infra.galaxy.talos.client.SimpleTopicAbnormalCallback;
import com.xiaomi.infra.galaxy.talos.client.TalosClientConfig;
import com.xiaomi.infra.galaxy.talos.producer.ProducerNotActiveException;
import com.xiaomi.infra.galaxy.talos.producer.TalosProducer;
import com.xiaomi.infra.galaxy.talos.producer.TalosProducerConfig;
import com.xiaomi.infra.galaxy.talos.producer.UserMessageCallback;
import com.xiaomi.infra.galaxy.talos.producer.UserMessageResult;
import com.xiaomi.infra.galaxy.talos.thrift.DescribeTopicRequest;
import com.xiaomi.infra.galaxy.talos.thrift.Message;
import com.xiaomi.infra.galaxy.talos.thrift.Topic;
import com.xiaomi.infra.galaxy.talos.thrift.TopicTalosResourceName;

public class TalosProducerDemo {
  private static final Logger LOG = LoggerFactory.getLogger(TalosProducerDemo.class);

  // callback for producer success/fail to put message
  private static class MyMessageCallback implements UserMessageCallback {
    // count when success
    @Override
    public void onSuccess(UserMessageResult userMessageResult) {
      long count = successPutNumber.addAndGet(
          userMessageResult.getMessageList().size());

      for (Message message : userMessageResult.getMessageList()) {
        LOG.info("success to put message: " + new String(message.getMessage()));
      }
      LOG.info("success to put message: " + count + " so far.");
    }

    // retry when failed
    @Override
    public void onError(UserMessageResult userMessageResult) {
      try {
        for (Message message : userMessageResult.getMessageList()) {
          LOG.info("failed to put message: " + message + " we will retry to put it.");
        }
        talosProducer.addUserMessage(userMessageResult.getMessageList());
      } catch (ProducerNotActiveException e) {
        e.printStackTrace();
      }
    }
  }

  private static final String propertyFileName = "$your_propertyFile";
  private static final String accessKey = "$your_team_accessKey";
  private static final String accessSecret = "$your_team_accessSecret";
  private static final String topicName = "testTopic";
  private static final int toPutMsgNumber = 7;
  private static final AtomicLong successPutNumber = new AtomicLong(0);

  private TalosClientConfig clientConfig;
  private TalosProducerConfig producerConfig;
  private Credential credential;
  private TalosAdmin talosAdmin;

  private TopicTalosResourceName topicTalosResourceName;
  private static TalosProducer talosProducer;

  public TalosProducerDemo() throws Exception {
    // init client config by put $your_propertyFile in your classpath
    // with the content of:
    /*
      galaxy.talos.service.endpoint=$talosServiceURI
    */
    clientConfig = new TalosClientConfig(propertyFileName);
    producerConfig = new TalosProducerConfig(propertyFileName);

    // credential
    credential = new Credential();
    credential.setSecretKeyId(accessKey)
        .setSecretKey(accessSecret)
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
    talosProducer = new TalosProducer(producerConfig, credential,
        topicTalosResourceName, new SimpleTopicAbnormalCallback(),
        new MyMessageCallback());

    List<Message> messageList = new ArrayList<Message>();
    while (true) {
      for (int i = 0; i < toPutMsgNumber; ++i) {
        String messageStr = "message id: " + i + ": this message is a text string.";
        Message message = new Message(ByteBuffer.wrap(messageStr.getBytes()));
        messageList.add(message);
      }
      talosProducer.addUserMessage(messageList);
      try {
        Thread.sleep(10000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    // when call shutdown function,
    // the producer will wait all the messages in buffer to send to server

    //talosProducer.shutdown();
  }

  public static void main(String[] args) throws Exception {
    TalosProducerDemo producerDemo = new TalosProducerDemo();
    // add message list to producer continuously
    producerDemo.start();
  }
}
