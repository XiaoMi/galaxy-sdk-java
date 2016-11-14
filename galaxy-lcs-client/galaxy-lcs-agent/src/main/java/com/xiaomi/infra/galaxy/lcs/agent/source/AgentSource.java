/**
 * Copyright 2016, Xiaomi.
 * All rights reserved.
 * Author: xiajun@xiaomi.com
 */

package com.xiaomi.infra.galaxy.lcs.agent.source;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.infra.galaxy.lcs.agent.channel.AgentChannel;
import com.xiaomi.infra.galaxy.lcs.thrift.GalaxyLCSException;
import com.xiaomi.infra.galaxy.talos.thrift.Message;

public abstract class AgentSource {
  private static final Logger LOG = LoggerFactory.getLogger(AgentSource.class);

  protected String topicName;

  protected AgentSource(String topicName) {
    this.topicName = topicName;
  }

  abstract public void start() throws GalaxyLCSException;

  abstract public void stop() throws GalaxyLCSException;
}
