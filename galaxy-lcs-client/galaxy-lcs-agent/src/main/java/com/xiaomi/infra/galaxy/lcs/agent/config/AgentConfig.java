/**
 * Copyright 2016, Xiaomi.
 * All rights reserved.
 * Author: xiajun@xiaomi.com
 */

package com.xiaomi.infra.galaxy.lcs.agent.config;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.google.common.base.Preconditions;

import com.xiaomi.infra.galaxy.lcs.agent.model.ChannelType;
import com.xiaomi.infra.galaxy.lcs.agent.model.SinkType;
import com.xiaomi.infra.galaxy.lcs.agent.model.SourceType;
import com.xiaomi.infra.galaxy.lcs.log.core.utils.ConfigureChecker;
import com.xiaomi.infra.galaxy.talos.client.TalosClientConfig;

public class AgentConfig {
  public static final int DEFAULT_HTTP_MAX_TOTAL_CONNECTIONS = 8;
  public static final int DEFAULT_HTTP_MAX_TOTAL_CONNECTIONS_PER_ROUTE = 8;
  public static final int DEFAILT_HTTP_CONNECTION_TIMEOUT_MILLIS = 5000;

  private int httpMaxTotalConnections;
  private int httpMaxTotalConnectionsPerRoute;
  private int httpConnectionTimeout;

  private Map<String, AgentSourceConfig> sourceConfigMap;
  private Map<String, AgentChannelConfig> channelConfigMap;
  private Map<String, AgentSinkConfig> sinkConfigMap;
  private Map<String, AgentStreamConfig> streamConfigMap;


  public AgentConfig(String confFileName) {
    this(TalosClientConfig.loadProperties(confFileName));
  }

  public AgentConfig(Properties properties) {
    this.sourceConfigMap = new HashMap<String, AgentSourceConfig>();
    this.channelConfigMap = new HashMap<String, AgentChannelConfig>();
    this.sinkConfigMap = new HashMap<String, AgentSinkConfig>();
    this.streamConfigMap = new HashMap<String, AgentStreamConfig>();

    init(properties);
  }

  public static int getDefaultHttpMaxTotalConnections() {
    return DEFAULT_HTTP_MAX_TOTAL_CONNECTIONS;
  }

  public int getHttpMaxTotalConnections() {
    return httpMaxTotalConnections;
  }

  public int getHttpMaxTotalConnectionsPerRoute() {
    return httpMaxTotalConnectionsPerRoute;
  }

  public int getHttpConnectionTimeout() {
    return httpConnectionTimeout;
  }

  public Map<String, AgentStreamConfig> getStreamConfigMap() {
    return streamConfigMap;
  }

  private void init(Properties properties) {
    loadHttpConnectionConfig(properties);
    loadAllSourceConfig(properties);
    loadAllChannelConfig(properties);
    loadAllSinkConfig(properties);
    loadAllStreamConfig(properties);

    checkConfig();
  }

  private void loadHttpConnectionConfig(Properties properties) {
    httpMaxTotalConnections = Integer.valueOf(properties.getProperty(
        AgentConfigKey.HTTP_MAX_TOTAL_CONNECTIONS,
        String.valueOf(DEFAULT_HTTP_MAX_TOTAL_CONNECTIONS)));
    httpMaxTotalConnectionsPerRoute = Integer.valueOf(properties.getProperty(
        AgentConfigKey.HTTP_MAX_TOTAL_CONNECTIONS_PER_ROUTE,
        String.valueOf(DEFAULT_HTTP_MAX_TOTAL_CONNECTIONS_PER_ROUTE)));
    httpConnectionTimeout = Integer.valueOf(properties.getProperty(
        AgentConfigKey.HTTP_CONNECTION_TIMEOUT_MILLIS,
        String.valueOf(DEFAILT_HTTP_CONNECTION_TIMEOUT_MILLIS)));

    ConfigureChecker.checkConfigureRange("httpMaxTotalConnections",
        httpMaxTotalConnections, 1, 64);
    ConfigureChecker.checkConfigureRange("httpMaxTotalConnectionsPerRoute",
        httpMaxTotalConnectionsPerRoute, 1, 16);
    ConfigureChecker.checkConfigureRange("httpConnectionTimeout",
        httpConnectionTimeout, 3 * 1000, 300 * 1000);
  }

  private void loadAllSourceConfig(Properties properties) {
    Set<String> sourceNameSet = loadNameSet(properties, AgentConfigKey.SOURCE_NAME_LIST, "SourceName");

    for (String sourceName: sourceNameSet) {
      loadSourceConfig(properties, sourceName);
    }
  }

  private void loadSourceConfig(Properties properties, String sourceName) {
    SourceType sourceType = SourceType.valueOf(getString(properties,
        sourceName + AgentConfigKey.SOURCE_TYPE_SUFFIX));

    AgentSourceConfig sourceConfig;
    switch (sourceType) {
      case FILE:
        sourceConfig = loadFileSourceConfig(properties, sourceName);
        break;
      case THRIFT:
        sourceConfig = loadThriftSourceConfig(properties, sourceName);
        break;
      default:
        throw new RuntimeException("Unsupported SourceType: " + sourceType + " for Agent");
    }

    sourceConfigMap.put(sourceName, sourceConfig);
  }

  private FileSourceConfig loadFileSourceConfig(Properties properties, String sourceName) {
    FileSourceConfig sourceConfig = new FileSourceConfig();
    sourceConfig.setRootFilePath(getString(properties, sourceName + AgentConfigKey.FILE_SOURCE_ROOT_PATH_SUFFIX));

    sourceConfig.setCheckPeriodMillis(getLong(properties,
        sourceName + AgentConfigKey.FILE_SOURCE_CHECK_PERIOD_MILLIS_SUFFIX,
        AgentConfigKey.DEFAULT_FILE_SOURCE_CHECK_PERIOD_MILLIS));

    return sourceConfig;
  }

  private ThriftSourceConfig loadThriftSourceConfig(Properties properties, String sourceName) {
    ThriftSourceConfig sourceConfig = new ThriftSourceConfig();
    sourceConfig.setAgentPort(getInt(properties, sourceName + AgentConfigKey.THRIFT_SOURCE_PORT_SUFFIX));

    sourceConfig.setSelectorThreadNumber(getInt(properties,
        sourceName + AgentConfigKey.THRIFT_SOURCE_SELECTOR_THREAD_NUMBER_SUFFIX,
        AgentConfigKey.DEFAULT_THRIFT_SOURCE_SELECTOR_THREAD_NUMBER));
    sourceConfig.setWorkerThreadNumber(getInt(properties,
        sourceName + AgentConfigKey.THRIFT_SOURCE_WORKER_THREAD_NUMBER_SUFFIX,
        AgentConfigKey.DEFAULT_THRIFT_SOURCE_WORKER_THREAD_NUMBER));

    return sourceConfig;
  }

  private void loadAllChannelConfig(Properties properties) {
    Set<String> channelNameSet = loadNameSet(properties, AgentConfigKey.CHANNEL_NAME_LIST, "ChannelName");

    for (String channelName : channelNameSet) {
      loadChannelConfig(properties, channelName);
    }
  }

  private void loadChannelConfig(Properties properties, String channelName) {
    ChannelType channelType = ChannelType.valueOf(getString(properties,
        channelName + AgentConfigKey.CHANGE_TYPE_SUFFIX));

    AgentChannelConfig channelConfig;
    switch (channelType) {
      case NON_MEMORY:
        channelConfig = loadNonMemoryChannelConfig(properties, channelName);
        break;
      case MEMORY:
        channelConfig = loadMemoryChannelConfig(properties, channelName);
        break;
      default:
        throw new RuntimeException("Unsupported ChannelType: " + channelType + " for Agent");
    }

    channelConfigMap.put(channelName, channelConfig);
  }

  private NonMemoryChannelConfig loadNonMemoryChannelConfig(Properties properties, String channelName) {
    NonMemoryChannelConfig channelConfig = new NonMemoryChannelConfig();

    return channelConfig;
  }

  private MemoryChannelConfig loadMemoryChannelConfig(Properties properties, String channelName) {
    MemoryChannelConfig channelConfig = new MemoryChannelConfig();

    channelConfig.setMaxMessageBufferBytes(getLong(properties,
        channelName + AgentConfigKey.MEMORY_CHANNEL_MAX_MESSAGE_BUFFER_BYTES_SUFFIX,
        AgentConfigKey.DEFAULT_MEMORY_CHANNEL_MAX_MESSAGE_BUFFER_BYTES));
    channelConfig.setFlushMessageBytes(getLong(properties,
        channelName + AgentConfigKey.MEMORY_CHANNEL_FLUSH_MESSAGE_BYTES_SUFFIX,
        AgentConfigKey.DEFAULT_MEMORY_CHANNEL_FLUSH_MESSAGE_BYTES));
    channelConfig.setFlushMessageNumber(getLong(properties,
        channelName + AgentConfigKey.MEMORY_CHANNEL_FLUSH_MESSAGE_NUMBER_SUFFIX,
        AgentConfigKey.DEFAULT_MEMORY_CHANNEL_FLUSH_MESSAGE_NUMBER));
    channelConfig.setFlushMessageIntervalMillis(getLong(properties,
        channelName + AgentConfigKey.MEMORY_CHANNEL_FLUSH_MESSAGE_INTERVAL_MILLIS_SUFFIX,
        AgentConfigKey.DEFAULT_MEMORY_CHANNEL_FLUSH_MESSAGE_INTERVAL_MILLIS));

    return channelConfig;
  }

  private void loadAllSinkConfig(Properties properties) {
    Set<String> sinkNameSet = loadNameSet(properties, AgentConfigKey.SINK_NAME_LIST, "SinkName");

    for (String sinkName : sinkNameSet) {
      loadSinkConfig(properties, sinkName);
    }
  }

  private void loadSinkConfig(Properties properties, String sinkName) {
    // we only support TALOS sinkType;
    SinkType sinkType = SinkType.valueOf(getString(properties,
        sinkName + AgentConfigKey.SINK_TYPE_SUFFIX));

    AgentSinkConfig sinkConfig;
    switch (sinkType) {
      case TALOS:
        sinkConfig = loadTalosSinkConfig(properties, sinkName);
        break;
      case FILE:
        sinkConfig = loadFileSinkConfig(properties, sinkName);
        break;
      default:
        throw new RuntimeException("Unsupported SinkType: " + sinkType + " for Agent");
    }

    sinkConfigMap.put(sinkName, sinkConfig);
  }

  private TalosSinkConfig loadTalosSinkConfig(Properties properties, String sinkName) {
    TalosSinkConfig sinkConfig = new TalosSinkConfig();

    sinkConfig.setTalosEndpoint(getString(properties, sinkName + AgentConfigKey.TALOS_SINK_EMDPOINT_SUFFIX));
    sinkConfig.setSecretKeyId(getString(properties, sinkName + AgentConfigKey.TALOS_SINK_SECRET_KEY_ID_SUFFIX));
    sinkConfig.setSecretKey(getString(properties, sinkName + AgentConfigKey.TALOS_SINK_SECRET_KEY_SUFFIX));
    sinkConfig.setSecretType(getString(properties, sinkName + AgentConfigKey.TALOS_SINK_SECRET_KEY_TYPE_SUFFIX));

    sinkConfig.setRefreshPeriodMillis(getLong(properties,
        sinkName + AgentConfigKey.TALOS_SINK_REFRESH_PERIOD_MILLIS_SUFFIX,
        AgentConfigKey.DEFAULT_TALOS_SINK_REFRESH_PERIOD_MILLIS));

    return sinkConfig;
  }

  private FileSinkConfig loadFileSinkConfig(Properties properties, String sinkName) {
    FileSinkConfig sinkConfig = new FileSinkConfig();
    sinkConfig.setRootFilePath(getString(properties, sinkName + AgentConfigKey.FILE_SINK_ROOT_PATH_SUFFIX));

    sinkConfig.setMaxFileNumber(getLong(properties,
        sinkName + AgentConfigKey.FILE_SINK_MAX_FILE_NUMBER_SUFFIX,
        AgentConfigKey.DEFAULT_FILE_SINK_MAX_FILE_NUMBER));
    sinkConfig.setRotateFileBytes(getLong(properties,
        sinkName + AgentConfigKey.FILE_SINK_ROTATE_FILE_BYTES_SUFFIX,
        AgentConfigKey.DEFAULT_FILE_SINK_ROTATE_FILE_BYTES));
    sinkConfig.setRotateFileIntervalMillis(getLong(properties,
        sinkName + AgentConfigKey.FILE_SINK_ROTATE_FILE_INTERVAL_MILLIS_SUFFIX,
        AgentConfigKey.DEFAULT_FILE_SINK_ROTATE_FILE_INTERVAL_MILLIS));

    return sinkConfig;
  }

  private void loadAllStreamConfig(Properties properties) {
    Set<String> topicNameSet = loadNameSet(properties, AgentConfigKey.TOPIC_NAME_LIST, "TopicName");

    for (String topicName : topicNameSet) {
      loadStreamConfig(properties, topicName);
    }
  }

  private void loadStreamConfig(Properties properties, String topicName) {
    String sourceName = getString(properties, topicName + AgentConfigKey.TOPIC_SOURCE_NAME_SUFFIX);
    String channelName = getString(properties, topicName + AgentConfigKey.TOPIC_CHANNEL_NAME_SUFFIX);
    String sinkName = getString(properties, topicName + AgentConfigKey.TOPIC_SINK_NAME_SUFFIX);

    AgentSourceConfig sourceConfig = sourceConfigMap.get(sourceName);
    if (sourceConfig == null) {
      throw new RuntimeException("Topic: " + topicName + " sourceConfig for sourceName: " + sourceName + " not exist");
    }

    AgentChannelConfig channelConfig = channelConfigMap.get(channelName);
    if (channelConfig == null) {
      throw new RuntimeException("Topic: " + topicName + " channelConfig for channelName: " + channelName + " not exist");
    }

    AgentSinkConfig sinkConfig = sinkConfigMap.get(sinkName);
    if (sinkConfig == null) {
      throw new RuntimeException("Topic: " + topicName + " sinkConfig for sinkName: " + sinkName + " not exist");
    }

    AgentStreamConfig streamConfig = new AgentStreamConfig(topicName, sourceConfig, channelConfig, sinkConfig);
    streamConfigMap.put(topicName, streamConfig);
  }

  private void checkConfig() {
    // check thriftSource number, only one is allowed;
    List<String> thriftSourceNameList = new ArrayList<String>();
    for (Map.Entry<String, AgentSourceConfig> entry : sourceConfigMap.entrySet()) {
      if (entry.getValue().getSourceType() == SourceType.THRIFT) {
        thriftSourceNameList.add(entry.getKey());
      }
    }
    if (thriftSourceNameList.size() > 1) {
      throw new RuntimeException("Only one ThriftSource is allowed, there are " +
          "too many ThriftSource, they are " + thriftSourceNameList);
    }

    // check FileSource must use NON_MEMORY channel
    // check ThriftSource must not use NON_MEMORY channel
    for (Map.Entry<String, AgentStreamConfig> entry : streamConfigMap.entrySet()) {
      AgentStreamConfig streamConfig = entry.getValue();
      if (streamConfig.getSourceConfig().getSourceType() == SourceType.FILE) {
        Preconditions.checkArgument(
            streamConfig.getChannelConfig().getChannelType() == ChannelType.NON_MEMORY,
            "FileSource must use NON_MEMORY channel, please reset for topic: " + entry.getKey());
      } else if (streamConfig.getSourceConfig().getSourceType() == SourceType.THRIFT) {
        // right now we only NON_MEMORY channel;
        Preconditions.checkArgument(
            streamConfig.getChannelConfig().getChannelType() != ChannelType.NON_MEMORY,
            "ThriftSource must not use NON_MEMORY channel, please reset for topic: " + entry.getKey());
      }
    }
  }

  private Set<String> loadNameSet(Properties properties, String key, String type) {
    String nameStr = getString(properties, key);

    String[] names = nameStr.split(",");
    Set<String> nameSet = new HashSet<String>(names.length);
    for (String topicName : names) {
      if (nameSet.contains(topicName)) {
        throw new RuntimeException("Repeated " + type + ": " + topicName + " for Agent");
      }
      nameSet.add(topicName);
    }

    return nameSet;
  }


  private String getString(Properties properties, String key) {
    String value = properties.getProperty(key);
    if (value == null) {
      throw new RuntimeException("Please set \"" + key
          + "\" for Agent");
    }

    return value;
  }


  private int getInt(Properties properties, String key) {
    String value = properties.getProperty(key);
    if (value == null) {
      throw new RuntimeException("Please set \"" + key
          + "\" for Agent");
    }

    return Integer.valueOf(value);
  }

  private int getInt(Properties properties, String key, int defaultValue) {
    String value = properties.getProperty(key);
    if (value == null) {
      return defaultValue;
    }

    return Integer.valueOf(value);
  }

  private long getLong(Properties properties, String key) {
    String value = properties.getProperty(key);
    if (value == null) {
      throw new RuntimeException("Please set \"" + key
          + "\" for Agent");
    }

    return Long.valueOf(value);
  }

  private long getLong(Properties properties, String key, long defaultValue) {
    String value = properties.getProperty(key);
    if (value == null) {
      return defaultValue;
    }

    return Long.valueOf(value);
  }

  @Override
  public String toString() {
    return "AgentConfig{" +
        "sourceConfigMap=" + sourceConfigMap +
        ", channelConfigMap=" + channelConfigMap +
        ", sinkConfigMap=" + sinkConfigMap +
        ", streamConfigMap=" + streamConfigMap +
        '}';
  }
}
