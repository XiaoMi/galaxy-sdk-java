/**
 * Copyright 2016, Xiaomi.
 * All rights reserved.
 * Author: xiajun@xiaomi.com
 */

package com.xiaomi.infra.galaxy.lcs.agent.sink;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.http.client.HttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.infra.galaxy.lcs.agent.config.AgentSinkConfig;
import com.xiaomi.infra.galaxy.lcs.agent.config.TalosSinkConfig;
import com.xiaomi.infra.galaxy.lcs.thrift.ErrorCode;
import com.xiaomi.infra.galaxy.lcs.thrift.GalaxyLCSException;
import com.xiaomi.infra.galaxy.rpc.thrift.Credential;
import com.xiaomi.infra.galaxy.rpc.thrift.UserType;
import com.xiaomi.infra.galaxy.talos.admin.TalosAdmin;
import com.xiaomi.infra.galaxy.talos.client.TalosClientConfig;
import com.xiaomi.infra.galaxy.talos.client.TalosClientConfigKeys;
import com.xiaomi.infra.galaxy.talos.client.TalosClientFactory;
import com.xiaomi.infra.galaxy.talos.producer.SimpleProducer;
import com.xiaomi.infra.galaxy.talos.producer.TalosProducerConfig;
import com.xiaomi.infra.galaxy.talos.thrift.DescribeTopicRequest;
import com.xiaomi.infra.galaxy.talos.thrift.GalaxyTalosException;
import com.xiaomi.infra.galaxy.talos.thrift.Message;
import com.xiaomi.infra.galaxy.talos.thrift.Topic;
import com.xiaomi.infra.galaxy.talos.thrift.TopicAndPartition;
import com.xiaomi.infra.galaxy.talos.thrift.TopicTalosResourceName;

public class TalosSink extends AgentSink {
  private static final Logger LOG = LoggerFactory.getLogger(TalosSink.class);

  private TalosSinkConfig sinkConfig;
  private HttpClient httpClient;

  private Random random;
  private Credential credential;
  private Properties properties;
  private TalosClientConfig talosClientConfig;
  private TalosClientFactory talosClientFactory;
  private TalosAdmin talosAdmin;
  private Topic topic;
  private final List<SimpleProducer> simpleProducerList;
  private long lastRefreshTimestamp;

  public TalosSink(String topicName, AgentSinkConfig sinkConfig, HttpClient httpClient) {
    super(topicName);

    if (!(sinkConfig instanceof TalosSinkConfig)) {
      throw new RuntimeException("Wrong AgentSinkConfig type: " +
          sinkConfig.getSinkType() + " for TalosAgentSink");
    }

    this.sinkConfig = (TalosSinkConfig)sinkConfig;
    this.httpClient = httpClient;

    random = new Random();

    // init credential;
    credential = new Credential();
    credential.setSecretKeyId(this.sinkConfig.getSecretKeyId())
        .setSecretKey(this.sinkConfig.getSecretKey())
        .setType(UserType.valueOf(this.sinkConfig.getSecretType()));

    // init properties;
    properties = new Properties();
    properties.setProperty(TalosClientConfigKeys.GALAXY_TALOS_SERVICE_ENDPOINT,
        ((TalosSinkConfig) sinkConfig).getTalosEndpoint());

    // init talosClientConfig
    talosClientConfig = new TalosClientConfig(properties);

    // init talosClientFactory
    talosClientFactory = new TalosClientFactory(talosClientConfig, credential, httpClient);

    // init talosAdmin
    talosAdmin = new TalosAdmin(talosClientFactory);

    // init simpleProducerList;
    simpleProducerList = new ArrayList<SimpleProducer>();

    lastRefreshTimestamp = System.currentTimeMillis();
  }

  @Override
  public void start() throws GalaxyLCSException {
    for (int i = 0; i < 5; ++i) {
      try {
        describeTopic();
      } catch (GalaxyLCSException e) {
        // when topic not exist or we have retry many times, we will throw Exception;
        if (e.errorCode == ErrorCode.INVALID_TOPIC || i == 4) {
          throw e;
        }
      }
    }
    updateSimpleProducer();
  }

  @Override
  public void stop() throws GalaxyLCSException {
    cleanSimpleProducer();
  }

  @Override
  public void writeMessage(List<Message> messageList) throws GalaxyLCSException {
    SimpleProducer simpleProducer = getSimpleProducer();
    try {
      simpleProducer.putMessageList(messageList);
      LOG.error("Topic: " + topicName + " putMessage: [" + messageList.size() + "] success");
    } catch (Exception e) {
      LOG.error("Topic: " + topicName + " putMessage failed", e);
      if (isTopicNotExist(e)) {
        throw new GalaxyLCSException(ErrorCode.INVALID_TOPIC).setDetails(e.toString());
      }
      throw new GalaxyLCSException(ErrorCode.TALOS_OPERATION_FAILED).setDetails(e.toString());
    }

    if (System.currentTimeMillis() - lastRefreshTimestamp >
        sinkConfig.getRefreshPeriodMillis()) {
      refreshTopic();
    }
  }

  private void refreshTopic() {
    lastRefreshTimestamp = System.currentTimeMillis();

    try {
      describeTopic();
      updateSimpleProducer();
    } catch (GalaxyLCSException e) {
      LOG.error("Topic: " + topicName + " refreshTopic failed", e);
    }
  }

  private void describeTopic() throws GalaxyLCSException {
    DescribeTopicRequest request = new DescribeTopicRequest();
    request.setTopicName(topicName);
    try {
      topic = talosAdmin.describeTopic(request);
    } catch (Exception e) {
      LOG.error("Describe topic: " + topicName + " failed", e);
      if (isTopicNotExist(e)) {
        throw new GalaxyLCSException(ErrorCode.INVALID_TOPIC).setDetails(e.toString());
      } else {
        throw new GalaxyLCSException(ErrorCode.TALOS_OPERATION_FAILED).setDetails(e.toString());
      }
    }
  }


  private void updateSimpleProducer() {
    synchronized (simpleProducerList) {
      simpleProducerList.clear();

      int partitionNumber = topic.getTopicAttribute().getPartitionNumber();
      TopicTalosResourceName resourceName = topic.getTopicInfo().getTopicTalosResourceName();

      // right now talos only support add partition, so we can do the follow logic;
      for (int index = 0; index < partitionNumber; ++index) {
        TopicAndPartition topicAndPartition = new TopicAndPartition();
        topicAndPartition.setTopicName(topicName);
        topicAndPartition.setPartitionId(index);
        topicAndPartition.setTopicTalosResourceName(resourceName);

        simpleProducerList.add(new SimpleProducer(new TalosProducerConfig(properties),
            topicAndPartition, credential));
      }
    }
  }

  private void cleanSimpleProducer() {
    synchronized (simpleProducerList) {
      simpleProducerList.clear();
    }
  }

  private SimpleProducer getSimpleProducer() {
    synchronized (simpleProducerList) {
      if (simpleProducerList.size() == 0) {
        return null;
      }

      int index = random.nextInt(simpleProducerList.size());
      return simpleProducerList.get(index);
    }
  }

  private boolean isTopicNotExist(Exception e) {
    if ((e instanceof GalaxyTalosException) &&
        (((GalaxyTalosException) e).getErrorCode() ==
            com.xiaomi.infra.galaxy.talos.thrift.ErrorCode.TOPIC_NOT_EXIST)) {
      return true;
    }

    return false;
  }
}
