/**
 * Copyright 2016, Xiaomi.
 * All rights reserved.
 * Author: xiajun@xiaomi.com
 */

package com.xiaomi.infra.galaxy.lcs.agent.config;


import com.xiaomi.infra.galaxy.lcs.agent.model.ChannelType;

public abstract class AgentChannelConfig {
  protected ChannelType channelType;

  public AgentChannelConfig(ChannelType channelType) {
    this.channelType = channelType;
  }

  public ChannelType getChannelType() {
    return channelType;
  }

  public void setChannelType(ChannelType channelType) {
    this.channelType = channelType;
  }
}
