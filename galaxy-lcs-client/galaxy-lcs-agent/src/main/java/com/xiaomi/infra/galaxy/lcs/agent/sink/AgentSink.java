/**
 * Copyright 2016, Xiaomi.
 * All rights reserved.
 * Author: xiajun@xiaomi.com
 */

package com.xiaomi.infra.galaxy.lcs.agent.sink;

import java.util.List;

import com.xiaomi.infra.galaxy.lcs.thrift.GalaxyLCSException;
import com.xiaomi.infra.galaxy.talos.thrift.Message;

public abstract class AgentSink {
  protected String topicName;
  public AgentSink(String topicName) {
    this.topicName = topicName;
  }

  abstract public void start() throws GalaxyLCSException;

  abstract public void stop() throws GalaxyLCSException;

  abstract public void writeMessage(List<Message> messageList) throws GalaxyLCSException;
}
