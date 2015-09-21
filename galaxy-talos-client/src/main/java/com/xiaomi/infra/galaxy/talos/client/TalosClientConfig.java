/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: yongxing@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.client;

import org.apache.hadoop.conf.Configuration;

import com.xiaomi.infra.galaxy.talos.common.config.TalosConfigKeys;

public class TalosClientConfig {
  private int maxRetry;
  private int clientTimeout;
  private int clientConnTimeout;
  private int adminOperationTimeout;
  private String serviceEndpoint;
  private String secureServiceEndpoint;
  private int maxTotalConnections;
  private int maxTotalConnectionsPerRoute;

  public TalosClientConfig(Configuration configuration) {
    maxRetry = configuration.getInt(
        TalosConfigKeys.GALAXY_TALOS_CLIENT_MAX_RETRY,
        TalosConfigKeys.GALAXY_TALOS_CLIENT_MAX_RETRY_DEFAULT);
    clientTimeout = configuration.getInt(
        TalosConfigKeys.GALAXY_TALOS_CLIENT_TIMEOUT_MILLI_SECS,
        TalosConfigKeys.GALAXY_TALOS_CLIENT_TIMEOUT_MILLI_SECS_DEFAULT);
    clientConnTimeout = configuration.getInt(
        TalosConfigKeys.GALAXY_TALOS_CLIENT_CONN_TIMECOUT_MILLI_SECS,
        TalosConfigKeys.GALAXY_TALOS_CLIENT_CONN_TIMECOUT_MILLI_SECS_DEFAULT);
    adminOperationTimeout = configuration.getInt(
        TalosConfigKeys.GALAXY_TALOS_CLIENT_ADMIN_TIMEOUT_MILLI_SECS,
        TalosConfigKeys.GALAXY_TALOS_CLIENT_ADMIN_TIMEOUT_MILLI_SECS_DEFAULT);
    serviceEndpoint = configuration.get(
        TalosConfigKeys.GALAXY_TALOS_SERVICE_ENDPOINT,
        TalosConfigKeys.GALAXY_TALOS_SERVICE_ENDPOINT_DEFAULT);
    secureServiceEndpoint = configuration.get(
        TalosConfigKeys.GALAXY_TALOS_SECURE_SERVICE_ENDPOINT,
        TalosConfigKeys.GALAXY_TALOS_SECURE_SERVICE_ENDPOINT_DEFAULT);
    maxTotalConnections = configuration.getInt(
        TalosConfigKeys.GALAXY_TALOS_HTTP_MAX_TOTAL_CONNECTION,
        TalosConfigKeys.GALAXY_TALOS_HTTP_MAX_TOTAL_CONNECTION_DEFAULT);
    maxTotalConnectionsPerRoute = configuration.getInt(
        TalosConfigKeys.GALAXY_TALOS_HTTP_MAX_TOTAL_CONNECTION_PER_ROUTE,
        TalosConfigKeys.GALAXY_TALOS_HTTP_MAX_TOTAL_CONNECTION_PER_ROUTE_DEFAULT);
  }

  public int getMaxRetry() {
    return maxRetry;
  }

  public int getClientTimeout() {
    return clientTimeout;
  }

  public int getClientConnTimeout() {
    return clientConnTimeout;
  }

  public int getAdminOperationTimeout() {
    return adminOperationTimeout;
  }

  public String getServiceEndpoint() {
    return serviceEndpoint;
  }

  public String getSecureServiceEndpoint() {
    return secureServiceEndpoint;
  }

  public int getMaxTotalConnections() {
    return maxTotalConnections;
  }

  public int getMaxTotalConnectionsPerRoute() {
    return maxTotalConnectionsPerRoute;
  }
}
