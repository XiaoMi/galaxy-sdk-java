/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: yongxing@xiaomi.com
 */

package com.xiaomi.infra.codelab.talos;

import java.util.List;

import java.util.concurrent.atomic.AtomicLong;

import libthrift091.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.infra.galaxy.rpc.thrift.Credential;
import com.xiaomi.infra.galaxy.rpc.thrift.UserType;
import com.xiaomi.infra.galaxy.talos.client.SimpleTopicAbnormalCallback;
import com.xiaomi.infra.galaxy.talos.consumer.MessageCheckpointer;
import com.xiaomi.infra.galaxy.talos.consumer.MessageProcessor;
import com.xiaomi.infra.galaxy.talos.consumer.MessageProcessorFactory;
import com.xiaomi.infra.galaxy.talos.consumer.TalosConsumer;
import com.xiaomi.infra.galaxy.talos.consumer.TalosConsumerConfig;
import com.xiaomi.infra.galaxy.talos.thrift.MessageAndOffset;
import com.xiaomi.infra.galaxy.talos.thrift.TopicAndPartition;

public class TalosConsumerDemo {
  private static final Logger LOG = LoggerFactory.getLogger(TalosConsumerDemo.class);

  // callback for consumer to process messages, that is, consuming logic
  private static class MyMessageProcessor implements MessageProcessor {
    @Override
    public void init(TopicAndPartition topicAndPartition, long messageOffset) {

    }

    @Override
    public void process(List<MessageAndOffset> messages, MessageCheckpointer messageCheckpointer) {
      try {
        // add your process logic for 'messages'
        for (MessageAndOffset messageAndOffset : messages) {
          LOG.info("Message content: " + new String(
              messageAndOffset.getMessage().getMessage()));
        }

        long count = successGetNumber.addAndGet(messages.size());
        LOG.info("Consuming total data so far: " + count);

        /** if user has set 'galaxy.talos.consumer.checkpoint.auto.commit' to false,
         * then you can call the 'checkpoint' to commit the list of messages.
         */
        //messageCheckpointer.checkpoint();
      } catch (Throwable throwable) {
        LOG.error("process error, ", throwable);
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

  private static final String propertyFileName = "$your_propertyFile";
  private static final String accessKey = "$your_team_accessKey";
  private static final String accessSecret = "$your_team_accessSecret";
  private static final String topicName = "testTopic";
  private static final AtomicLong successGetNumber = new AtomicLong(0);

  private static final String clientPrefix = "departmentName-";
  private static final String consumerGroup = "groupName";

  private TalosConsumerConfig consumerConfig;
  private Credential credential;
  private TalosConsumer talosConsumer;

  public TalosConsumerDemo() {
    // init client config by put $your_propertyFile in your classpath
    // with the content of:
    /*
      galaxy.talos.service.endpoint=$talosServiceURI
    */
    consumerConfig = new TalosConsumerConfig(propertyFileName);

    // credential
    credential = new Credential();
    credential.setSecretKeyId(accessKey)
        .setSecretKey(accessSecret)
        .setType(UserType.DEV_XIAOMI);
  }

  public void start() throws TException {
    talosConsumer = new TalosConsumer(consumerGroup, consumerConfig,
        credential, topicName, new MyMessageProcessorFactory(),
        clientPrefix, new SimpleTopicAbnormalCallback());
  }

  public static void main(String[] args) throws Exception {
    TalosConsumerDemo consumerDemo = new TalosConsumerDemo();
    consumerDemo.start();
  }
}
