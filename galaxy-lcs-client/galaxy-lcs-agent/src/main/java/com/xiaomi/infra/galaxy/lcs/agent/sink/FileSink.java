/**
 * Copyright 2016, Xiaomi.
 * All rights reserved.
 * Author: xiajun@xiaomi.com
 */

package com.xiaomi.infra.galaxy.lcs.agent.sink;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.infra.galaxy.lcs.agent.config.FileSinkConfig;
import com.xiaomi.infra.galaxy.lcs.agent.config.AgentSinkConfig;
import com.xiaomi.infra.galaxy.lcs.agent.logger.AgentLogger;
import com.xiaomi.infra.galaxy.lcs.log.core.file.LCSFileWriter;
import com.xiaomi.infra.galaxy.lcs.thrift.ErrorCode;
import com.xiaomi.infra.galaxy.lcs.thrift.GalaxyLCSException;
import com.xiaomi.infra.galaxy.talos.thrift.Message;

public class FileSink extends AgentSink {
  private static final Logger LOG = LoggerFactory.getLogger(AgentSink.class);

  private FileSinkConfig sinkConfig;
  private LCSFileWriter fileWriter;


  public FileSink(String topicName, AgentSinkConfig sinkConfig) {
    super(topicName);

    if (!(sinkConfig instanceof FileSinkConfig)) {
      throw new RuntimeException("Wrong AgentSinkConfig type: " +
          sinkConfig.getSinkType() + " for FileSink");
    }

    this.sinkConfig = (FileSinkConfig)sinkConfig;
  }

  @Override
  public void start() throws GalaxyLCSException {
    this.fileWriter = new LCSFileWriter(new AgentLogger(LOG), topicName,
        this.sinkConfig.getRootFilePath(), this.sinkConfig.getMaxFileNumber(),
        this.sinkConfig.getRotateFileBytes(), this.sinkConfig.getRotateFileIntervalMillis());
    this.fileWriter.init();
  }

  @Override
  public void stop() throws GalaxyLCSException {
    fileWriter.close();
  }

  @Override
  public void writeMessage(List<Message> messageList) throws GalaxyLCSException {
    try {
      fileWriter.writeMessage(messageList);
    } catch (IOException e) {
      throw new GalaxyLCSException(ErrorCode.IO_ERROR).setDetails(e.toString());
    }
  }
}
