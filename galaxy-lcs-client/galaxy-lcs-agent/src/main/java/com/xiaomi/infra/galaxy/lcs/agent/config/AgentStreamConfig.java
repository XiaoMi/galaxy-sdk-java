/**
 * Copyright 2016, Xiaomi.
 * All rights reserved.
 * Author: xiajun@xiaomi.com
 */

package com.xiaomi.infra.galaxy.lcs.agent.config;

import com.xiaomi.infra.galaxy.lcs.agent.model.ChannelType;
import com.xiaomi.infra.galaxy.lcs.agent.model.SourceType;

public class AgentStreamConfig {
  private final String topicName;
  private final AgentSourceConfig sourceConfig;
  private final AgentChannelConfig channelConfig;
  private final AgentSinkConfig sinkConfig;

  public AgentStreamConfig(String topicName, AgentSourceConfig sourceConfig,
      AgentChannelConfig channelConfig, AgentSinkConfig sinkConfig) {
    this.topicName = topicName;
    this.sourceConfig = sourceConfig;
    this.channelConfig = channelConfig;
    this.sinkConfig = sinkConfig;

    if (sourceConfig.getSourceType() == SourceType.FILE &&
        channelConfig.getChannelType() != ChannelType.NON_MEMORY) {
      throw new RuntimeException("FileSource must use NON_MEMORY CHANNEL");
    }
  }

  public String getTopicName() {
    return topicName;
  }

  public AgentSourceConfig getSourceConfig() {
    return sourceConfig;
  }

  public AgentChannelConfig getChannelConfig() {
    return channelConfig;
  }

  public AgentSinkConfig getSinkConfig() {
    return sinkConfig;
  }

  @Override
  public String toString() {
    return "AgentStreamConfig{" +
        "topicName='" + topicName + '\'' +
        ", sourceConfig=" + sourceConfig +
        ", channelConfig=" + channelConfig +
        ", sinkConfig=" + sinkConfig +
        '}';
  }
}
