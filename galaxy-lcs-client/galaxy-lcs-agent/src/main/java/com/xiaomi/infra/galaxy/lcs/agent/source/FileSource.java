/**
 * Copyright 2016, Xiaomi.
 * All rights reserved.
 * Author: xiajun@xiaomi.com
 */

package com.xiaomi.infra.galaxy.lcs.agent.source;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.infra.galaxy.lcs.agent.channel.AgentChannel;
import com.xiaomi.infra.galaxy.lcs.agent.config.FileSourceConfig;
import com.xiaomi.infra.galaxy.lcs.agent.config.AgentSourceConfig;
import com.xiaomi.infra.galaxy.lcs.agent.logger.AgentLogger;
import com.xiaomi.infra.galaxy.lcs.log.core.file.LCSFileReader;
import com.xiaomi.infra.galaxy.lcs.thrift.GalaxyLCSException;
import com.xiaomi.infra.galaxy.talos.thrift.Message;

public class FileSource extends AgentSource {
  private static final Logger LOG = LoggerFactory.getLogger(FileSource.class);

  private FileSourceConfig sourceConfig;
  private AgentChannel agentChannel;

  private AtomicBoolean stopped;
  private LCSFileReader talosFileReader;
  private ScheduledExecutorService scheduledThread;

  public FileSource(String topicName, AgentSourceConfig sourceConfig, AgentChannel agentChannel) {
    super(topicName);

    if (!(sourceConfig instanceof FileSourceConfig)) {
      throw new RuntimeException("Wrong AgentSourceConfig type: " +
          sourceConfig.getSourceType() + " for FileSource");
    }

    this.sourceConfig = (FileSourceConfig)sourceConfig;
    this.agentChannel = agentChannel;

    stopped = new AtomicBoolean(false);
    talosFileReader = new LCSFileReader(new AgentLogger(LOG),
        this.topicName, this.sourceConfig.getRootFilePath());
    scheduledThread = Executors.newSingleThreadScheduledExecutor();
  }

  @Override
  public void start() throws GalaxyLCSException {
    stopped.set(false);
    talosFileReader.initTransaction();

    scheduledThread.submit(new Runnable() {
      @Override
      public void run() {
        readMessage();
      }
    });
  }

  private void readMessage() {
    while (!stopped.get()) {
      talosFileReader.startTransaction();
      List<Message> messageList = talosFileReader.take();
      if (messageList.size() == 0) {
        talosFileReader.commitTransaction();
        // when there are no more message, we will wait for checkPeriod Millis;
        try {
          Thread.sleep(sourceConfig.getCheckPeriodMillis());
        } catch (InterruptedException e) {
        }
      } else {
        try {
          agentChannel.putMessage(messageList);
          talosFileReader.commitTransaction();
        } catch (GalaxyLCSException e) {
          talosFileReader.rollbackTransaction();
          LOG.info("Topic: " + topicName + " put [" + messageList.size() +
              "] to AgentChannel failed", e);
        }
      }
    }
  }

  @Override
  public void stop() throws GalaxyLCSException {
    stopped.set(true);
    scheduledThread.shutdown();
    try {
      scheduledThread.awaitTermination(sourceConfig.getCheckPeriodMillis() * 2, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
    }

    talosFileReader.closeTransaction();
  }
}
