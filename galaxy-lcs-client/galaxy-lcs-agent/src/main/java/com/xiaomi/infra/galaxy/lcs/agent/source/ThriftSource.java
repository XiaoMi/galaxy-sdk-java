/**
 * Copyright 2016, Xiaomi.
 * All rights reserved.
 * Author: xiajun@xiaomi.com
 */

package com.xiaomi.infra.galaxy.lcs.agent.source;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Preconditions;
import libthrift091.TException;
import libthrift091.protocol.TCompactProtocol;
import libthrift091.server.TServer;
import libthrift091.server.TThreadedSelectorServer;
import libthrift091.transport.TFramedTransport;
import libthrift091.transport.TNonblockingServerSocket;
import libthrift091.transport.TNonblockingServerTransport;
import libthrift091.transport.TTransportException;
import org.apache.log4j.Logger;

import com.xiaomi.infra.galaxy.lcs.agent.channel.AgentChannel;
import com.xiaomi.infra.galaxy.lcs.agent.config.AgentSourceConfig;
import com.xiaomi.infra.galaxy.lcs.agent.config.ThriftSourceConfig;
import com.xiaomi.infra.galaxy.lcs.thrift.ErrorCode;
import com.xiaomi.infra.galaxy.lcs.thrift.GalaxyLCSException;
import com.xiaomi.infra.galaxy.lcs.thrift.LCSThriftService;
import com.xiaomi.infra.galaxy.lcs.thrift.Record;
import com.xiaomi.infra.galaxy.talos.client.serialization.MessageSerialization;
import com.xiaomi.infra.galaxy.talos.thrift.Message;

public class ThriftSource extends AgentSource {
  private class LCSAgentServiceProcessor implements LCSThriftService.Iface {
    @Override
    public void Record(Record record) throws TException {
      List<Message> messageList = new ArrayList<Message>(record.getDataSize());

      AgentChannel agentChannel = topicChannelMap.get(record.getTopicName());
      if (agentChannel == null) {
        LOG.info("Topic: " + record.getTopicName() + " putMessage: " + messageList.size() + " failed");
        throw new GalaxyLCSException(ErrorCode.INVALID_TOPIC).setDetails(
            "Topic: " + record.getTopicName() + " is not registered");
      }

      try {
        for (ByteBuffer byteBuffer : record.getData()) {
          ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(byteBuffer.array());
          DataInputStream dataInputStream = new DataInputStream(byteArrayInputStream);
          Message message = MessageSerialization.deserializeMessage(dataInputStream);
          messageList.add(message);
        }
      } catch (Exception e) {
        LOG.info("Topic: " + record.getTopicName() + " putMessage: " + messageList.size() + " failed", e);
        throw new GalaxyLCSException(ErrorCode.DESERIALIZE_FAILED).setDetails(e.toString());
      }

      try {
        agentChannel.putMessage(messageList);
      } catch (Exception e) {
        LOG.info("Topic: " + record.getTopicName() + " putMessage: " + messageList.size() + " failed", e);
        if (e instanceof GalaxyLCSException) {
          throw (GalaxyLCSException)e;
        }
        else {
          throw new GalaxyLCSException(ErrorCode.UNEXCEPTED_ERROR).setDetails(e.toString());
        }
      }

      LOG.info("Topic: " + record.getTopicName() + " putMessage: " + messageList.size() + " success");
    }
  }

  private static ThriftSource INSTANCE = null;
  private static final Logger LOG = Logger.getLogger(ThriftSource.class);

  private ThriftSourceConfig sourceConfig;

  private TreeMap<String, AgentChannel> topicChannelMap;
  private AtomicBoolean started;
  private AtomicBoolean stopped;

  private TServer thriftServer;
  private ScheduledExecutorService thriftServerThread;

  public static ThriftSource getINSTANCE(AgentSourceConfig sourceConfig) {
    if (INSTANCE != null) {
      return INSTANCE;
    }

    INSTANCE = new ThriftSource(sourceConfig);
    return INSTANCE;
  }

  private ThriftSource(AgentSourceConfig sourceConfig) {
    super(null);

    if (!(sourceConfig instanceof ThriftSourceConfig)) {
      throw new RuntimeException("Wrong AgentSourceConfig type: " +
          sourceConfig.getSourceType() + " for ThriftSource");
    }

    this.sourceConfig = (ThriftSourceConfig)sourceConfig;

    this.topicChannelMap = new TreeMap<String, AgentChannel>();
    this.started = new AtomicBoolean(false);
    this.stopped = new AtomicBoolean(false);
  }

  public void addTopicChannel(String topicName, AgentChannel agentChannel) {
    Preconditions.checkArgument(started.compareAndSet(false, false),
        "You can't add topic and channel to ThriftSource after it started");
    topicChannelMap.put(topicName, agentChannel);
  }

  @Override
  public void start() throws GalaxyLCSException {
    Preconditions.checkArgument(topicChannelMap.size() != 0);
    if (!started.compareAndSet(false, true)) {
      return;
    }

    try {
      LCSThriftService.Processor lcsAgentProcessor = new
          LCSThriftService.Processor<LCSThriftService.Iface>(new LCSAgentServiceProcessor());
      TNonblockingServerTransport transport = new TNonblockingServerSocket(
          this.sourceConfig.getAgentPort());
      TThreadedSelectorServer.Args args = new TThreadedSelectorServer.Args(transport);
      args.selectorThreads(this.sourceConfig.getSelectorThreadNumber());
      args.workerThreads(this.sourceConfig.getWorkerThreadNumber());
      // set maxLength in case of OOM
      args.maxReadBufferBytes = 150 * 1024 * 1024;
      args.transportFactory(new TFramedTransport.Factory(50 * 1024 * 1024));
      args.protocolFactory(new TCompactProtocol.Factory());
      args.processor(lcsAgentProcessor);
      thriftServer = new TThreadedSelectorServer(args);

      thriftServerThread = Executors.newSingleThreadScheduledExecutor();
    } catch (TTransportException e) {
      throw new GalaxyLCSException(ErrorCode.IO_ERROR).setDetails(e.toString());
    }

    thriftServerThread.submit(new Runnable() {
      @Override
      public void run() {
        thriftServer.serve();
      }
    });
  }

  @Override
  public void stop() throws GalaxyLCSException {
    if (!stopped.compareAndSet(false, true)) {
      return;
    }

    thriftServer.stop();
    thriftServerThread.shutdown();
    try {
      thriftServerThread.wait();
    } catch (Exception e) {

    }

  }
}
