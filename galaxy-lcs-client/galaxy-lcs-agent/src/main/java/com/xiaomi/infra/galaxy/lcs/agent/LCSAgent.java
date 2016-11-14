/**
 * Copyright 2016, Xiaomi.
 * All rights reserved.
 * Author: xiajun@xiaomi.com
 */

package com.xiaomi.infra.galaxy.lcs.agent;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.http.client.HttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.infra.galaxy.lcs.agent.config.AgentConfig;
import com.xiaomi.infra.galaxy.lcs.agent.config.AgentStreamConfig;
import com.xiaomi.infra.galaxy.lcs.agent.stream.AgentStream;
import com.xiaomi.infra.galaxy.lcs.thrift.GalaxyLCSException;
import com.xiaomi.infra.galaxy.talos.client.TalosClientFactory;


public class LCSAgent {
  private static final Logger LOG = LoggerFactory.getLogger(LCSAgent.class);

  private AgentConfig agentConfig;
  private Map<String, AgentStream> agentStreamMap;
  private AtomicBoolean stopped;
  private HttpClient httpClient;


  public LCSAgent(String configFileName) {
    this.agentConfig = new AgentConfig(configFileName);

    init();
  }

  public LCSAgent(Properties properties) {
    this.agentConfig = new AgentConfig(properties);

    init();
  }

  private void init() {
    this.agentStreamMap = new HashMap<String, AgentStream>();
    this.stopped = new AtomicBoolean(false);
    // all TalosSink use the same httpClient;
    this.httpClient = TalosClientFactory.generateHttpClient(
        agentConfig.getHttpMaxTotalConnections(),
        agentConfig.getHttpMaxTotalConnectionsPerRoute(),
        agentConfig.getHttpConnectionTimeout());

    initAgentStream();
  }

  private void initAgentStream() {
    Map<String, AgentStreamConfig> streamConfigMap = agentConfig.getStreamConfigMap();
    for (Map.Entry<String, AgentStreamConfig> entry : streamConfigMap.entrySet()) {
      AgentStream agentStream = new AgentStream(entry.getValue(), httpClient);
      agentStreamMap.put(entry.getKey(), agentStream);
    }
  }

  public void start() throws GalaxyLCSException {
    stopped.set(false);

    for (Map.Entry<String, AgentStream> entry : agentStreamMap.entrySet()) {
      entry.getValue().start();
    }
  }

  public void join() {
    while (!stopped.get()) {
      try {
        Thread.sleep(10 * 1000);
      } catch (Exception e) {

      }
    }
  }

  public void stop() throws GalaxyLCSException {
    for (Map.Entry<String, AgentStream> entry : agentStreamMap.entrySet()) {
      entry.getValue().stop();
    }

    stopped.set(true);
  }

  public static void main(String[] args) throws Exception {
    String configFile = System.getProperty("lcs.agent.config.file");
    if (configFile == null) {
      throw new RuntimeException("Please set \"lcs.agent.config.file\" for TalosAgent");
    }
    LCSAgent agent = new LCSAgent(configFile);
    try {
      agent.start();
    } catch (Exception e) {
      LOG.error("start LCSAgent failed", e);
      System.exit(-1);
    }

    agent.join();
  }
}
