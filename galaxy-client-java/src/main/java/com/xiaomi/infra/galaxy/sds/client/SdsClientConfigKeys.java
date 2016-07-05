package com.xiaomi.infra.galaxy.sds.client;

/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: qiankai@xiaomi.com
 */
public class SdsClientConfigKeys {
  // client config
  /**
   * Constants for http/https rpc address
   */
  public static final String GALAXY_SDS_SERVICE_ENDPOINT =
      "galaxy.sds.service.endpoint";
  public static final String GALAXY_SDS_SERVICE_ENDPOINT_DEFAULT =
      "http://staging.sds.api.xiaomi.com";

  /**
   * The http client connection params
   */
  public static final String GALAXY_SDS_HTTP_MAX_TOTAL_CONNECTION =
      "galaxy.sds.http.max.total.connection";
  public static final int GALAXY_SDS_HTTP_MAX_TOTAL_CONNECTION_DEFAULT = 160;

  public static final String GALAXY_SDS_HTTP_MAX_TOTAL_CONNECTION_PER_ROUTE =
      "galaxy.sds.http.max.total.connection.per.route";
  public static final int GALAXY_SDS_HTTP_MAX_TOTAL_CONNECTION_PER_ROUTE_DEFAULT =
      160;

  /**
   * The client whether to retry
   */
  public static final String GALAXY_SDS_CLIENT_IS_RETRY =
      "galaxy.sds.client.is.retry";
  public static final boolean GALAXY_SDS_CLIENT_IS_RETRY_DEFAULT = false;

  /**
   * The client max retry times before throw exception
   */
  public static final String GALAXY_SDS_CLIENT_MAX_RETRY =
      "galaxy.sds.client.max.retry";
  public static final int GALAXY_SDS_CLIENT_MAX_RETRY_DEFAULT = 1;

  /**
   * The client timeout milli secs when write/read
   */
  public static final String GALAXY_SDS_CLIENT_TIMEOUT_MILLI_SECS =
      "galaxy.sds.client.timeout.milli.secs";
  public static final int GALAXY_SDS_CLIENT_TIMEOUT_MILLI_SECS_DEFAULT = 10000;

  /**
   * The client DDL operation timeout
   */
  public static final String GALAXY_SDS_CLIENT_ADMIN_TIMEOUT_MILLI_SECS =
      "galaxy.sds.client.admin.timeout.milli.secs";
  public static final int GALAXY_SDS_CLIENT_ADMIN_TIMEOUT_MILLI_SECS_DEFAULT =
      30000;

  /**
   * The client connection timeout
   */
  public static final String GALAXY_SDS_CLIENT_CONN_TIMECOUT_MILLI_SECS =
      "galaxy.sds.client.conn.timeout.milli.secs";
  public static final int GALAXY_SDS_CLIENT_CONN_TIMECOUT_MILLI_SECS_DEFAULT =
      5000;
}
