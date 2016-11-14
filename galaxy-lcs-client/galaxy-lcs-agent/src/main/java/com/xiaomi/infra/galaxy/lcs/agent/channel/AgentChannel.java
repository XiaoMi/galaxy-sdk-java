/**
 * Copyright 2016, Xiaomi.
 * All rights reserved.
 * Author: xiajun@xiaomi.com
 */

package com.xiaomi.infra.galaxy.lcs.agent.channel;

import java.util.List;

import com.xiaomi.infra.galaxy.lcs.agent.config.AgentChannelConfig;
import com.xiaomi.infra.galaxy.lcs.log.core.transaction.Transaction;
import com.xiaomi.infra.galaxy.lcs.thrift.GalaxyLCSException;
import com.xiaomi.infra.galaxy.talos.thrift.Message;

public interface AgentChannel {
  abstract public void start();
  abstract public void putMessage(List<Message> messageList) throws GalaxyLCSException;
  abstract public void stop();
}
