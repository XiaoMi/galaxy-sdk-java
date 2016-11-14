/**
 * Copyright 2016, Xiaomi.
 * All rights reserved.
 * Author: xiajun@xiaomi.com
 */

package com.xiaomi.infra.galaxy.lcs.agent.config;


import com.xiaomi.infra.galaxy.lcs.agent.model.ChannelType;

public class NonMemoryChannelConfig extends AgentChannelConfig {
  public NonMemoryChannelConfig() {
    super(ChannelType.NON_MEMORY);
  }

  @Override
  public String toString() {
    return super.toString();
  }
}
