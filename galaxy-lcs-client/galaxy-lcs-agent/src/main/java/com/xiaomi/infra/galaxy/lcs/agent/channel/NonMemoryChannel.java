/**
 * Copyright 2016, Xiaomi.
 * All rights reserved.
 * Author: xiajun@xiaomi.com
 */

package com.xiaomi.infra.galaxy.lcs.agent.channel;

import java.util.List;

import com.xiaomi.infra.galaxy.lcs.agent.config.AgentChannelConfig;
import com.xiaomi.infra.galaxy.lcs.agent.config.NonMemoryChannelConfig;
import com.xiaomi.infra.galaxy.lcs.agent.sink.AgentSink;
import com.xiaomi.infra.galaxy.lcs.thrift.GalaxyLCSException;
import com.xiaomi.infra.galaxy.talos.thrift.Message;

public class NonMemoryChannel implements AgentChannel {
  private String topicName;
  private NonMemoryChannelConfig channelConfig;
  private AgentSink agentSink;

  public NonMemoryChannel(String topicName, AgentChannelConfig channelConfig,
      AgentSink agentSink) {

    if (!(channelConfig instanceof NonMemoryChannelConfig)) {
      throw new RuntimeException("Wrong AgentChannelConfig type: " +
          channelConfig.getChannelType() + " for NonMemoryChannel");
    }

    this.topicName = topicName;
    this.channelConfig = (NonMemoryChannelConfig)channelConfig;
    this.agentSink = agentSink;
  }

  @Override
  public void putMessage(List<Message> messageList) throws GalaxyLCSException {
    try {
      agentSink.writeMessage(messageList);
    } catch (GalaxyLCSException e) {
      throw e;
    }
  }

  @Override
  public void start() {
  }

  @Override
  public void stop() {
  }
}
