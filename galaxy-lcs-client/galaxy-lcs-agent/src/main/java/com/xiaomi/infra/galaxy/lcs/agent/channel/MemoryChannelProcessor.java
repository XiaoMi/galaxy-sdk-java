/**
 * Copyright 2016, Xiaomi.
 * All rights reserved.
 * Author: xiajun@xiaomi.com
 */

package com.xiaomi.infra.galaxy.lcs.agent.channel;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.infra.galaxy.lcs.agent.sink.AgentSink;
import com.xiaomi.infra.galaxy.lcs.thrift.GalaxyLCSException;
import com.xiaomi.infra.galaxy.talos.thrift.Message;

public class MemoryChannelProcessor extends AgentChannelProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(MemoryChannelProcessor.class);

  private MemoryChannel memoryChannel;
  private ScheduledExecutorService scheduledThread;

  public MemoryChannelProcessor(String topicName, AgentChannel agentChannel, AgentSink agentSink) {
    super(topicName,  agentSink);
    if (!(agentChannel instanceof MemoryChannel)) {
      throw new RuntimeException("Wrong AgentChannel type: " +
          agentChannel.getClass() + " for NonMemoryChannel");
    }
    this.memoryChannel = (MemoryChannel)agentChannel;

    this.scheduledThread = Executors.newSingleThreadScheduledExecutor();
  }

  @Override
  public void start() throws GalaxyLCSException {
    scheduledThread.submit(new Runnable() {
      @Override
      public void run() {
        process();
      }
    });
  }

  @Override
  public void stop() throws GalaxyLCSException {
    scheduledThread.shutdown();
    try {
      scheduledThread.awaitTermination(600 * 1000, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
    }
  }

  @Override
  protected void process() {
    while (true) {
      try {
        memoryChannel.startTransaction();

        List<Message> messageList = memoryChannel.take();
        if (messageList.isEmpty()) {
          memoryChannel.commitTransaction();
          break;
        }

        try {
          agentSink.writeMessage(messageList);
          memoryChannel.commitTransaction();
        } catch (Exception e) {
          LOG.error("Topic: " + topicName + " process message failed, we will " +
              "rollback this transaction", e);
          memoryChannel.rollbackTransaction();
        }
      } catch (Exception e) {
        LOG.error("Topic: " + topicName + " process message failed", e);
      }
    }
  }
}
