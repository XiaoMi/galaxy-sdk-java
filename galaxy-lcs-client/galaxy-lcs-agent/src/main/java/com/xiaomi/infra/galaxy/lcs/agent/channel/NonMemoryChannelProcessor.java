/**
 * Copyright 2016, Xiaomi.
 * All rights reserved.
 * Author: xiajun@xiaomi.com
 */

package com.xiaomi.infra.galaxy.lcs.agent.channel;

import com.xiaomi.infra.galaxy.lcs.agent.sink.AgentSink;
import com.xiaomi.infra.galaxy.lcs.thrift.GalaxyLCSException;

public class NonMemoryChannelProcessor extends AgentChannelProcessor {
  public NonMemoryChannelProcessor(String topicName, AgentSink agentSink) {
    super(topicName, agentSink);
  }

  @Override
  public void start() throws GalaxyLCSException {

  }

  @Override
  public void stop() throws GalaxyLCSException {

  }

  @Override
  public void process() {

  }
}
