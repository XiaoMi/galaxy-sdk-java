/**
 * Copyright 2016, Xiaomi.
 * All rights reserved.
 * Author: xiajun@xiaomi.com
 */

package com.xiaomi.infra.galaxy.lcs.log.core.thrift;

import libthrift091.protocol.TCompactProtocol;
import libthrift091.protocol.TProtocol;
import libthrift091.transport.TFramedTransport;
import libthrift091.transport.TSocket;
import libthrift091.transport.TTransport;
import libthrift091.transport.TTransportException;

import com.xiaomi.infra.galaxy.lcs.log.core.ILogger;
import com.xiaomi.infra.galaxy.lcs.thrift.LCSThriftService;

public class LCSThriftClient {
  private ILogger logger;
  private String lcsAgentHostname;
  private int lcsAgentPort;

  private String lcsAgentInfo;
  private LCSThriftService.Client client;

  public LCSThriftClient(ILogger logger, String lcsAgentHostname, int lcsAgentPort) {
    this.logger = logger;
    this.lcsAgentHostname = lcsAgentHostname;
    this.lcsAgentPort = lcsAgentPort;

    this.lcsAgentInfo = lcsAgentHostname + ":" + lcsAgentPort;
    this.client = createLCSThriftClient();
  }

  public String getLcsAgentInfo() {
    return lcsAgentInfo;
  }

  public LCSThriftService.Client getClient() {
    if (client == null) {
      client = createLCSThriftClient();
    }

    return client;
  }

  public void resetClient() {
    this.client = null;
  }

  private LCSThriftService.Client createLCSThriftClient() {
    TSocket socket = new TSocket(
        lcsAgentHostname, lcsAgentPort, 4096 * 512);
    TTransport transport = new TFramedTransport(socket);
    try {
      transport.open();
    } catch (TTransportException e) {
      logger.info("Failed to create client for LCSAgent: " + lcsAgentHostname +
          ":" + lcsAgentPort, e);
      return null;
    }
    TProtocol protocol = new TCompactProtocol(transport);
    return new LCSThriftService.Client(protocol);
  }
}
