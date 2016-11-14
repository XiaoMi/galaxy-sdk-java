/**
 * Copyright 2016, Xiaomi.
 * All rights reserved.
 * Author: xiajun@xiaomi.com
 */

package com.xiaomi.infra.galaxy.lcs.log.core.appender;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import libthrift091.TException;

import com.xiaomi.infra.galaxy.lcs.log.core.ILogger;
import com.xiaomi.infra.galaxy.lcs.log.core.thrift.LCSThriftClient;
import com.xiaomi.infra.galaxy.lcs.thrift.ErrorCode;
import com.xiaomi.infra.galaxy.lcs.thrift.GalaxyLCSException;
import com.xiaomi.infra.galaxy.lcs.thrift.LCSThriftService;
import com.xiaomi.infra.galaxy.lcs.thrift.Record;
import com.xiaomi.infra.galaxy.talos.client.Constants;
import com.xiaomi.infra.galaxy.talos.client.serialization.MessageSerialization;
import com.xiaomi.infra.galaxy.talos.client.serialization.MessageSerializationFactory;
import com.xiaomi.infra.galaxy.talos.thrift.Message;

public class LCSThriftAppenderImpl extends LCSAppender {
  private String lcsAgentHostname;
  private int lcsAgentPort;

  private int bufferSize;
  private ByteArrayOutputStream byteArrayOutputStream;
  private DataOutputStream dataOutputStream;

  private LCSThriftClient lcsThriftClient;
  public LCSThriftAppenderImpl(ILogger logger) {
    super(logger);

    this.lcsAgentHostname = "127.0.0.1";
    this.lcsAgentPort = -1;
    // ATTENTION: right we think serialized message never longer than
    // signalMessageBytes * 2, there is risk that message header is longer
    // than signalMessageBytes, but this will not happen in the foreseeable
    // future;
    this.bufferSize = Constants.TALOS_SINGLE_MESSAGE_BYTES_MAXIMAL * 2;
    this.byteArrayOutputStream = new ByteArrayOutputStream(bufferSize);
    this.dataOutputStream = new DataOutputStream(byteArrayOutputStream);
  }

  public String getLcsAgentHost() {
    return lcsAgentHostname;
  }

  public void setLcsAgentHost(String lcsAgentHostname) {
    this.lcsAgentHostname = lcsAgentHostname;
  }

  public int getLcsAgentPort() {
    return lcsAgentPort;
  }

  public void setLcsAgentPort(int lcsAgentPort) {
    this.lcsAgentPort = lcsAgentPort;
  }

  @Override
  protected void doStart() {
    if (lcsAgentPort == -1) {
      throw new RuntimeException("please set lcsAgentPort");
    }

    lcsThriftClient = new LCSThriftClient(logger, lcsAgentHostname, lcsAgentPort);
  }

  @Override
  protected void doFlushMessage(List<Message> messageList) throws Exception {
    Record record = new Record();
    record.setTopicName(topicName);

    List<ByteBuffer> dataList = new ArrayList<ByteBuffer>();
    try {
      for (Message message : messageList) {
        byteArrayOutputStream.reset();
        MessageSerialization.serializeMessage(message, dataOutputStream,
            MessageSerializationFactory.getDefaultMessageVersion());
        dataList.add(ByteBuffer.wrap(byteArrayOutputStream.toByteArray()));
      }
    } catch (IOException e) {
      logger.error("Topic:" + topicName + " serialize message failed", e);
      throw e;
    }

    record.setData(dataList);
    try {
      LCSThriftService.Client recordClient = lcsThriftClient.getClient();
      if (recordClient == null) {
        throw new GalaxyLCSException(ErrorCode.AGENT_NOT_READY);
      }
      lcsThriftClient.getClient().Record(record);
    } catch (GalaxyLCSException e) {
      logger.error("Topic: " + topicName + " sendMessage to LcsAgent: " +
          lcsThriftClient.getLcsAgentInfo() + " failed", e);
      throw e;
    } catch (TException e) {
      logger.error("Topic: " + topicName + " sendMessage to LcsAgent: " +
          lcsThriftClient.getLcsAgentInfo() + " failed, may be LcsAgent is not " +
          "ready", e);
      lcsThriftClient.resetClient();
      throw e;
    }
  }

  @Override
  protected void doPeriodCheck() {

  }

  @Override
  protected void doClose() {

  }
}
