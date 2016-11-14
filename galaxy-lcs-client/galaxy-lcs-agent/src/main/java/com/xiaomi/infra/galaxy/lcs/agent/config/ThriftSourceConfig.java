/**
 * Copyright 2016, Xiaomi.
 * All rights reserved.
 * Author: xiajun@xiaomi.com
 */

package com.xiaomi.infra.galaxy.lcs.agent.config;


import com.xiaomi.infra.galaxy.lcs.agent.model.SourceType;
import com.xiaomi.infra.galaxy.lcs.log.core.utils.ConfigureChecker;

public class ThriftSourceConfig extends AgentSourceConfig {
  private int agentPort;
  private int selectorThreadNumber;
  private int workerThreadNumber;

  public ThriftSourceConfig() {
    super(SourceType.THRIFT);
  }

  public int getAgentPort() {
    return agentPort;
  }

  public void setAgentPort(int agentPort) {
    ConfigureChecker.checkConfigureRange("agentPort", agentPort, 1, Integer.MAX_VALUE);

    this.agentPort = agentPort;
  }

  public int getSelectorThreadNumber() {
    return selectorThreadNumber;
  }

  public void setSelectorThreadNumber(int selectorThreadNumber) {
    ConfigureChecker.checkConfigureRange("selectorThreadNumber",
        selectorThreadNumber, 1, 8);
    this.selectorThreadNumber = selectorThreadNumber;
  }

  public int getWorkerThreadNumber() {
    return workerThreadNumber;
  }

  public void setWorkerThreadNumber(int workerThreadNumber) {
    ConfigureChecker.checkConfigureRange("workerThreadNumber",
        workerThreadNumber, 1, 32);

    this.workerThreadNumber = workerThreadNumber;
  }

  @Override
  public String toString() {
    return "ThriftSourceConfig{" +
        "agentPort=" + agentPort +
        '}';
  }
}
