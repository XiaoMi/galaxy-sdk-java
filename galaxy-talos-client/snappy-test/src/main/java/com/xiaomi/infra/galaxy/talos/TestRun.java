/**
 * Copyright 2016, Xiaomi.
 * All rights reserved.
 * Author: xiajun@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos;

import java.util.List;
import java.util.Properties;

import com.xiaomi.infra.galaxy.rpc.thrift.Credential;
import com.xiaomi.infra.galaxy.rpc.thrift.UserType;
import com.xiaomi.infra.galaxy.talos.client.TalosClientConfigKeys;
import com.xiaomi.infra.galaxy.talos.client.Utils;
import com.xiaomi.infra.galaxy.talos.consumer.SimpleConsumer;
import com.xiaomi.infra.galaxy.talos.consumer.TalosConsumerConfig;
import com.xiaomi.infra.galaxy.talos.thrift.MessageAndOffset;
import com.xiaomi.infra.galaxy.talos.thrift.TopicAndPartition;
import com.xiaomi.infra.galaxy.talos.thrift.TopicTalosResourceName;

public class TestRun {
  public TestRun() {
  }

  public void run() throws Exception {
    Properties properties = new Properties();
    properties.setProperty(TalosClientConfigKeys.GALAXY_TALOS_SERVICE_ENDPOINT,
        "http://staging-cnbj1-talos.api.xiaomi.net:80");
    TalosConsumerConfig talosConsumerConfig = new TalosConsumerConfig(properties);

    // setup topicAndParition;
    TopicAndPartition topicAndPartition = new TopicAndPartition(
        Utils.getTopicNameByResourceName("167019#multi-test1#2b6d721519ec40b4a88ca06201835d1a"),
        new TopicTalosResourceName("167019#multi-test1#2b6d721519ec40b4a88ca06201835d1a"),
        0);

    // setup credential;
    Credential credential = new Credential();
    credential.setSecretKeyId("5801738598039")
        .setSecretKey("GvbhBGVx7MHMOExV8yqMYg==")
        .setType(UserType.DEV_XIAOMI);

    // setup simpleConsumer;
    SimpleConsumer simpleConsumer = new SimpleConsumer(talosConsumerConfig, topicAndPartition, credential);
    List<MessageAndOffset> messageAndOffsetList = simpleConsumer.fetchMessage(10000, 1000);
  }
}
