/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: yongxing@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.client;

public class TalosClientConfigKeys {
  // client config

  /**
   * Constants for http/https rpc address
   */
  public static final String GALAXY_TALOS_SERVICE_ENDPOINT =
      "galaxy.talos.service.endpoint";
  public static final String GALAXY_TALOS_SERVICE_ENDPOINT_DEFAULT =
      "http://talos.api.xiaomi.com";

  public static final String GALAXY_TALOS_SECURE_SERVICE_ENDPOINT =
      "galaxy.talos.secure.service.endpoint";
  public static final String GALAXY_TALOS_SECURE_SERVICE_ENDPOINT_DEFAULT =
      "https://talos.api.xiaomi.com";

  /**
   * The http client connection params
   */
  public static final String GALAXY_TALOS_HTTP_MAX_TOTAL_CONNECTION =
      "galaxy.talos.http.max.total.connection";
  public static final int GALAXY_TALOS_HTTP_MAX_TOTAL_CONNECTION_DEFAULT = 2;

  public static final String GALAXY_TALOS_HTTP_MAX_TOTAL_CONNECTION_PER_ROUTE =
      "galaxy.talos.http.max.total.connection.per.route";
  public static final int GALAXY_TALOS_HTTP_MAX_TOTAL_CONNECTION_PER_ROUTE_DEFAULT = 2;

  /**
   * The client whether to retry
   */
  public static final String GALAXY_TALOS_CLIENT_IS_RETRY =
      "galaxy.talos.client.is.retry";
  public static final boolean GALAXY_TALOS_CLIENT_IS_RETRY_DEFAULT = false;

  /**
   * The client max retry times before throw exception
   */
  public static final String GALAXY_TALOS_CLIENT_MAX_RETRY =
      "galaxy.talos.client.max.retry";
  public static final int GALAXY_TALOS_CLIENT_MAX_RETRY_DEFAULT = 1;

  /**
   * The client timeout milli secs when write/read
   */
  public static final String GALAXY_TALOS_CLIENT_TIMEOUT_MILLI_SECS =
      "galaxy.talos.client.timeout.milli.secs";
  public static final int GALAXY_TALOS_CLIENT_TIMEOUT_MILLI_SECS_DEFAULT = 10000;

  /**
   * The client DDL operation timeout
   */
  public static final String GALAXY_TALOS_CLIENT_ADMIN_TIMEOUT_MILLI_SECS =
      "galaxy.talos.client.admin.timeout.milli.secs";
  public static final int GALAXY_TALOS_CLIENT_ADMIN_TIMEOUT_MILLI_SECS_DEFAULT =
      30000;

  /**
   * The client connection timeout
   */
  public static final String GALAXY_TALOS_CLIENT_CONN_TIMECOUT_MILLI_SECS =
      "galaxy.talos.client.conn.timeout.milli.secs";
  public static final int GALAXY_TALOS_CLIENT_CONN_TIMECOUT_MILLI_SECS_DEFAULT =
      5000;


  // producer config

  /**
   * The producer buffered message number for one partition
   * if the total number exceed it, not allowed to addMessage for user
   */
  public static final String GALAXY_TALOS_PRODUCER_MAX_BUFFERED_MESSAGE_NUMBER =
      "galaxy.talos.producer.max.buffered.message.number";
  public static final int GALAXY_TALOS_PRODUCER_MAX_BUFFERED_MESSAGE_NUMBER_DEFAULT =
      1000000;

  /**
   * The producer buffered message bytes for one partition
   * if the total bytes exceed it, not allowed to addMessage for user
   */
  public static final String GALAXY_TALOS_PRODUCER_MAX_BUFFERED_MESSAGE_BYTES =
      "galaxy.talos.producer.max.buffered.message.bytes";
  public static final int GALAXY_TALOS_PRODUCER_MAX_BUFFERED_MESSAGE_BYTES_DEFAULT =
      500 * 1024 * 1024;

  /**
   * The producer buffered message time for one partition
   * if the oldest buffered message always exceed the time,
   * putMessage will always be called
   */
  public static final String GALAXY_TALOS_PRODUCER_MAX_BUFFERED_MILLI_SECS =
      "galaxy.talos.producer.max.buffered.milli.secs";
  public static final int GALAXY_TALOS_PRODUCER_MAX_BUFFERED_MILLI_SECS_DEFAULT = 1000;

  /**
   * The producer scan buffered message queue interval (milli secs)
   */
  public static final String GALAXY_TALOS_PRODUCER_SCAN_PARTITION_QUEUE_INTERVAL =
      "galaxy.talos.producer.scan.partition.queue.interval";
  public static final int GALAXY_TALOS_PRODUCER_SCAN_PARTITION_QUEUE_INTERVAL_DEFAULT =
      10;

  /**
   * The producer max number of message in each putMessage batch
   */
  public static final String GALAXY_TALOS_PRODUCER_MAX_PUT_MESSAGE_NUMBER =
      "galaxy.talos.producer.max.put.message.number";
  public static final int GALAXY_TALOS_PRODUCER_MAX_PUT_MESSAGE_NUMBER_DEFAULT =
      2000;

  /**
   * The producer max bytes of message in each putMessage batch
   */
  public static final String GALAXY_TALOS_PRODUCER_MAX_PUT_MESSAGE_BYTES =
      "galaxy.talos.producer.max.put.message.bytes";
  public static final int GALAXY_TALOS_PRODUCER_MAX_PUT_MESSAGE_BYTES_DEFAULT =
      4 * 1024 * 1024;

  /**
   * The producer thread pool number
   */
  public static final String GALAXY_TALOS_PRODUCER_THREAD_POOL_SIZE =
      "galaxy.talos.producer.thread.pool.size";
  public static final int GALAXY_TALOS_PRODUCER_THREAD_POOL_SIZE_DEFAULT = 16;

  /**
   * The producer scan/update partition number interval
   */
  public static final String GALAXY_TALOS_PRODUCER_CHECK_PARTITION_INTERVAL =
      "galaxy.talos.producer.check.partition.interval";
  public static final int GALAXY_TALOS_PRODUCER_CHECK_PARTITION_INTERVAL_DEFAULT =
      1000 * 60 * 3;

}
