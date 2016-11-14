/**
 * Copyright 2016, Xiaomi.
 * All rights reserved.
 * Author: xiajun@xiaomi.com
 */

package com.xiaomi.infra.galaxy.lcs.agent.stream;

import org.apache.http.client.HttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.infra.galaxy.lcs.agent.channel.AgentChannel;
import com.xiaomi.infra.galaxy.lcs.agent.channel.AgentChannelProcessor;
import com.xiaomi.infra.galaxy.lcs.agent.channel.MemoryChannel;
import com.xiaomi.infra.galaxy.lcs.agent.channel.MemoryChannelProcessor;
import com.xiaomi.infra.galaxy.lcs.agent.channel.NonMemoryChannel;
import com.xiaomi.infra.galaxy.lcs.agent.channel.NonMemoryChannelProcessor;
import com.xiaomi.infra.galaxy.lcs.agent.config.AgentChannelConfig;
import com.xiaomi.infra.galaxy.lcs.agent.config.AgentSinkConfig;
import com.xiaomi.infra.galaxy.lcs.agent.config.AgentSourceConfig;
import com.xiaomi.infra.galaxy.lcs.agent.config.AgentStreamConfig;
import com.xiaomi.infra.galaxy.lcs.agent.sink.AgentSink;
import com.xiaomi.infra.galaxy.lcs.agent.sink.FileSink;
import com.xiaomi.infra.galaxy.lcs.agent.sink.TalosSink;
import com.xiaomi.infra.galaxy.lcs.agent.source.AgentSource;
import com.xiaomi.infra.galaxy.lcs.agent.source.FileSource;
import com.xiaomi.infra.galaxy.lcs.agent.source.ThriftSource;
import com.xiaomi.infra.galaxy.lcs.thrift.GalaxyLCSException;


public class AgentStream {
  private static final Logger LOG = LoggerFactory.getLogger(AgentStream.class);
  private AgentStreamConfig streamConfig;
  private HttpClient httpClient;
  private String topicName;

  private AgentSink agentSink;
  private AgentChannel agentChannel;
  private AgentSource agentSource;
  private AgentChannelProcessor agentChannelProcessor;


  public AgentStream(AgentStreamConfig streamConfig, HttpClient httpClient) {
    this.streamConfig = streamConfig;
    this.httpClient = httpClient;
    this.topicName = streamConfig.getTopicName();

    LOG.info("AddAgentStream: " + streamConfig);

    initAgentSink();
    initAgentChannel(agentSink);
    initAgentSource(agentChannel);
  }

  public void start() throws GalaxyLCSException {
    agentSink.start();
    agentChannel.start();
    agentChannelProcessor.start();
    agentSource.start();
  }

  public void stop() throws GalaxyLCSException {
    agentSource.stop();
    agentChannel.stop();
    agentChannelProcessor.stop();
    agentSink.stop();
  }


  private void initAgentSink() {
    AgentSinkConfig sinkConfig = streamConfig.getSinkConfig();
    switch (sinkConfig.getSinkType()) {
      case TALOS:
        agentSink = new TalosSink(topicName, sinkConfig, httpClient);
        break;
      case FILE:
        agentSink = new FileSink(topicName, sinkConfig);
        break;
      default:
        throw new RuntimeException("Unsupported AgentSinkType: " + sinkConfig.getSinkType());
    }
  }

  private void initAgentChannel(AgentSink agentSink) {
    AgentChannelConfig channelConfig = streamConfig.getChannelConfig();
    switch (channelConfig.getChannelType()) {
      case NON_MEMORY:
        agentChannel = new NonMemoryChannel(topicName, channelConfig, agentSink);
        agentChannelProcessor = new NonMemoryChannelProcessor(topicName, agentSink);
        break;
      case MEMORY:
        agentChannel = new MemoryChannel(topicName, channelConfig);
        agentChannelProcessor = new MemoryChannelProcessor(topicName, agentChannel, agentSink);
        break;
      default:
        throw new RuntimeException("Unsupported AgentChangeType: " + channelConfig.getChannelType());
    }
  }


  private void initAgentSource(AgentChannel agentChannel) {
    AgentSourceConfig sourceConfig = streamConfig.getSourceConfig();
    switch (sourceConfig.getSourceType()) {
      case FILE:
        agentSource = new FileSource(topicName, sourceConfig, agentChannel);
        break;
      case THRIFT:
        ThriftSource thriftSource = ThriftSource.getINSTANCE(sourceConfig);
        thriftSource.addTopicChannel(topicName, agentChannel);
        agentSource = thriftSource;
        break;
      default:
        throw new RuntimeException("Unsupported AgentSourceType: " + sourceConfig.getSourceType());
    }
  }


}
