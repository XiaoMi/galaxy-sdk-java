/**
 * Copyright 2016, Xiaomi.
 * All rights reserved.
 * Author: xiajun@xiaomi.com
 */

package com.xiaomi.infra.galaxy.lcs.agent.config;

public class AgentConfigKey {
  // config for http connection
  public static final String HTTP_MAX_TOTAL_CONNECTIONS = "http.max.total.connections";
  public static final String HTTP_MAX_TOTAL_CONNECTIONS_PER_ROUTE = "http.max.total.connections.per.route";
  public static final String HTTP_CONNECTION_TIMEOUT_MILLIS = "http.connection.timeout.millis";


  // config for topic
  /**
   * Topic name for the agent to process;
   */
  public static final String TOPIC_NAME_LIST = "topic.name.list";
  public static final String TOPIC_SOURCE_NAME_SUFFIX = ".topic.source.name";
  public static final String TOPIC_CHANNEL_NAME_SUFFIX = ".topic.channel.name";
  public static final String TOPIC_SINK_NAME_SUFFIX = ".topic.sink.name";


  //////////////////////////////////////////////////////////////////////////////
  // config for source
  public static final String SOURCE_NAME_LIST = "source.name.list";

  // config for source type
  public static final String SOURCE_TYPE_SUFFIX = ".source.type";


  // config for file source
  public static final String FILE_SOURCE_ROOT_PATH_SUFFIX = ".file.source.root.path";
  public static final String FILE_SOURCE_CHECK_PERIOD_MILLIS_SUFFIX =
      ".file.source.check.period.millis";
  public static final long DEFAULT_FILE_SOURCE_CHECK_PERIOD_MILLIS = 60000L;

  // config for thrift source
  public static final String THRIFT_SOURCE_PORT_SUFFIX = ".thrift.source.port";
  public static final String THRIFT_SOURCE_SELECTOR_THREAD_NUMBER_SUFFIX = ".thrift.source.selector.thread.number";
  public static final int DEFAULT_THRIFT_SOURCE_SELECTOR_THREAD_NUMBER = 1;
  public static final String THRIFT_SOURCE_WORKER_THREAD_NUMBER_SUFFIX = ".thrift.source.worker.thread.number";
  public static final int DEFAULT_THRIFT_SOURCE_WORKER_THREAD_NUMBER = 4;


  //////////////////////////////////////////////////////////////////////////////
  // config for channel
  public static final String CHANNEL_NAME_LIST = "channel.name.list";

  // config for channel type;
  public static final String CHANGE_TYPE_SUFFIX = ".channel.type";

  // config for memory channel
  public static final String MEMORY_CHANNEL_MAX_MESSAGE_BUFFER_BYTES_SUFFIX = ".memory.channel.max.message.buffer.bytes";
  public static final long DEFAULT_MEMORY_CHANNEL_MAX_MESSAGE_BUFFER_BYTES = 500L * 1024 * 1024;
  public static final String MEMORY_CHANNEL_FLUSH_MESSAGE_BYTES_SUFFIX = ".memory.channel.flush.message.bytes";
  public static final long DEFAULT_MEMORY_CHANNEL_FLUSH_MESSAGE_BYTES = 1L * 1024 * 1024;
  public static final String MEMORY_CHANNEL_FLUSH_MESSAGE_NUMBER_SUFFIX = ".memory.channel.flush.message.number";
  public static final long DEFAULT_MEMORY_CHANNEL_FLUSH_MESSAGE_NUMBER = 1000L;
  public static final String MEMORY_CHANNEL_FLUSH_MESSAGE_INTERVAL_MILLIS_SUFFIX = ".memory.channel.flush.message.interval.millis";
  public static final long DEFAULT_MEMORY_CHANNEL_FLUSH_MESSAGE_INTERVAL_MILLIS = 1000L;



  //////////////////////////////////////////////////////////////////////////////
  // config for talos sink;
  public static final String SINK_NAME_LIST = "sink.name.list";

  // config for sink type
  public static final String SINK_TYPE_SUFFIX = ".sink.type";

  // config for talos sink
  public static final String TALOS_SINK_EMDPOINT_SUFFIX = ".talos.sink.endpoint";
  public static final String TALOS_SINK_SECRET_KEY_ID_SUFFIX = ".talos.sink.secret.key.id";
  public static final String TALOS_SINK_SECRET_KEY_SUFFIX = ".talos.sink.secret.key";
  public static final String TALOS_SINK_SECRET_KEY_TYPE_SUFFIX = ".talos.sink.secret.key.type";
  public static final String TALOS_SINK_REFRESH_PERIOD_MILLIS_SUFFIX = ".talos.sink.refresh.period.millis";
  public static final long DEFAULT_TALOS_SINK_REFRESH_PERIOD_MILLIS = 300L * 1000;

  // config for file sink
  public static final String FILE_SINK_ROOT_PATH_SUFFIX = ".file.sink.root.path";
  public static final String FILE_SINK_MAX_FILE_NUMBER_SUFFIX = ".file.sink.max.file.number";
  public static final long DEFAULT_FILE_SINK_MAX_FILE_NUMBER = 10000;
  public static final String FILE_SINK_ROTATE_FILE_BYTES_SUFFIX = ".file.sink.rotate.file.byts";
  public static final long DEFAULT_FILE_SINK_ROTATE_FILE_BYTES = 10 * 1024 * 1024;
  public static final String FILE_SINK_ROTATE_FILE_INTERVAL_MILLIS_SUFFIX = ".file.sink.rotate.interval.millis";
  public static final long DEFAULT_FILE_SINK_ROTATE_FILE_INTERVAL_MILLIS = 60000;

}
