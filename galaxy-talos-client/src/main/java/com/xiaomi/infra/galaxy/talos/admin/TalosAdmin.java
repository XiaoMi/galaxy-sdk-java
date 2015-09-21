/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: yongxing@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.admin;

import libthrift091.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.infra.galaxy.rpc.thrift.Credential;
import com.xiaomi.infra.galaxy.talos.client.TalosClientConfig;
import com.xiaomi.infra.galaxy.talos.client.TalosClientFactory;
import com.xiaomi.infra.galaxy.talos.thrift.CreateTopicRequest;
import com.xiaomi.infra.galaxy.talos.thrift.CreateTopicResponse;
import com.xiaomi.infra.galaxy.talos.thrift.DescribeTopicRequest;
import com.xiaomi.infra.galaxy.talos.thrift.DescribeTopicResponse;
import com.xiaomi.infra.galaxy.talos.thrift.GalaxyTalosException;
import com.xiaomi.infra.galaxy.talos.thrift.Topic;
import com.xiaomi.infra.galaxy.talos.thrift.TopicAttribute;
import com.xiaomi.infra.galaxy.talos.thrift.TopicService;
import com.xiaomi.infra.galaxy.talos.thrift.TopicTalosResourceName;

public class TalosAdmin {
  private static final Logger LOG = LoggerFactory.getLogger(TalosAdmin.class);
  private TopicService.Iface topicClient;

  public TalosAdmin(TalosClientConfig talosClientConfig) {
    new TalosAdmin(talosClientConfig, new Credential());
  }

  public TalosAdmin(TalosClientConfig talosClientConfig, Credential credential) {
    TalosClientFactory talosClientFactory =
        new TalosClientFactory(talosClientConfig, credential);
    new TalosAdmin(talosClientFactory);
  }

  public TalosAdmin(TalosClientFactory talosClientFactory) {
    topicClient = talosClientFactory.newTopicClient();
  }

  public TopicTalosResourceName createTopic(String topicName)
      throws GalaxyTalosException, TException {
    TopicAttribute topicAttribute = new TopicAttribute();
    CreateTopicRequest createTopicRequest = new CreateTopicRequest(
        topicName, topicAttribute);
    CreateTopicResponse createTopicResponse = topicClient.createTopic(
        createTopicRequest);
    LOG.info("Create topic: " + topicName + " success.");
    return createTopicResponse.getTopicInfo().getTopicTalosResourceName();
  }

  public Topic describeTopic(String topicName)
      throws GalaxyTalosException, TException {
    DescribeTopicRequest describeTopicRequest = new DescribeTopicRequest(topicName);
    DescribeTopicResponse describeTopicResponse = topicClient.describeTopic(
        describeTopicRequest);
    return new Topic(describeTopicResponse.getTopicInfo(),
        describeTopicResponse.getTopicAttribute(),
        describeTopicResponse.getTopicState());
  }
}
