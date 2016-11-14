/**
 * Copyright 2016, Xiaomi.
 * All rights reserved.
 * Author: xiajun@xiaomi.com
 */

package com.xiaomi.infra.galaxy.lcs.agent.channel;

import java.util.List;

import com.xiaomi.infra.galaxy.lcs.agent.sink.AgentSink;
import com.xiaomi.infra.galaxy.lcs.thrift.GalaxyLCSException;
import com.xiaomi.infra.galaxy.talos.thrift.Message;

public abstract class AgentChannelProcessor {
  protected String topicName;
  protected AgentSink agentSink;

  public AgentChannelProcessor(String topicName, AgentSink agentSink) {
    this.topicName = topicName;
    this.agentSink = agentSink;
  }

  abstract public void start() throws GalaxyLCSException;

  abstract public void stop() throws GalaxyLCSException;

  abstract protected void process();
}
