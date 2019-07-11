/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: yongxing@xiaomi.com
 */

package com.xiaomi.infra.codelab.talos;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import libthrift091.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.infra.galaxy.rpc.thrift.Credential;
import com.xiaomi.infra.galaxy.rpc.thrift.UserType;
import com.xiaomi.infra.galaxy.talos.producer.SimpleProducer;
import com.xiaomi.infra.galaxy.talos.producer.TalosProducerConfig;
import com.xiaomi.infra.galaxy.talos.thrift.Message;

public class TalosSimpleProducerDemo {
  private static final Logger LOG = LoggerFactory.getLogger(TalosSimpleProducerDemo.class);

  private static final String propertyFileName = "$your_propertyFile";
  private static final String accessKey = "$your_team_accessKey";
  private static final String accessSecret = "$your_team_accessSecret";
  private static final String topicName = "testTopic";
  private static final int partitionId = 0;
  private static final AtomicLong successPutNumber = new AtomicLong(0);

  private TalosProducerConfig producerConfig;
  private Credential credential;

  private static SimpleProducer simpleProducer;

  public TalosSimpleProducerDemo() {
    // init client config by put $your_propertyFile in your classpath
    // with the content of:
    /*
      galaxy.talos.service.endpoint=$talosServiceURI
    */
    producerConfig = new TalosProducerConfig(propertyFileName);

    // credential
    credential = new Credential();
    credential.setSecretKeyId(accessKey).setSecretKey(accessSecret)
        .setType(UserType.DEV_XIAOMI);
  }

  public void start() throws TException {
    // init producer
    simpleProducer = new SimpleProducer(producerConfig,
        topicName, partitionId, credential);

    String messageStr = "test message: this message is a text string.";
    Message message = new Message(ByteBuffer.wrap(messageStr.getBytes()));
    List<Message> messageList = new ArrayList<Message>();
    messageList.add(message);
    // a toy demo for putting messages to Talos server continuously
    // by using a infinite loop
    while (true) {
      try {
        simpleProducer.putMessageList(messageList);
        try {
          Thread.sleep(2000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      } catch (IOException e) {
        LOG.warn("put message failed, try again");
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
