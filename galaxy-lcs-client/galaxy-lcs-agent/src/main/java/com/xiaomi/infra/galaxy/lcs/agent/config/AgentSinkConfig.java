/**
 * Copyright 2016, Xiaomi.
 * All rights reserved.
 * Author: xiajun@xiaomi.com
 */

package com.xiaomi.infra.galaxy.lcs.agent.config;


import com.xiaomi.infra.galaxy.lcs.agent.model.SinkType;

public abstract class AgentSinkConfig {
  protected SinkType sinkType;

  public AgentSinkConfig(SinkType sinkType) {
    this.sinkType = sinkType;
  }

  public SinkType getSinkType() {
    return sinkType;
  }

  public void setSinkType(SinkType sinkType) {
    this.sinkType = sinkType;
  }
}
