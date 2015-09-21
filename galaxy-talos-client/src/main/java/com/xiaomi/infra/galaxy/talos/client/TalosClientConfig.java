/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: yongxing@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.client;

import org.apache.hadoop.conf.Configuration;

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
        TalosClientConfigKeys.GALAXY_TALOS_CLIENT_MAX_RETRY,
        TalosClientConfigKeys.GALAXY_TALOS_CLIENT_MAX_RETRY_DEFAULT);
    clientTimeout = configuration.getInt(
        TalosClientConfigKeys.GALAXY_TALOS_CLIENT_TIMEOUT_MILLI_SECS,
        TalosClientConfigKeys.GALAXY_TALOS_CLIENT_TIMEOUT_MILLI_SECS_DEFAULT);
    clientConnTimeout = configuration.getInt(
        TalosClientConfigKeys.GALAXY_TALOS_CLIENT_CONN_TIMECOUT_MILLI_SECS,
        TalosClientConfigKeys.GALAXY_TALOS_CLIENT_CONN_TIMECOUT_MILLI_SECS_DEFAULT);
    adminOperationTimeout = configuration.getInt(
        TalosClientConfigKeys.GALAXY_TALOS_CLIENT_ADMIN_TIMEOUT_MILLI_SECS,
        TalosClientConfigKeys.GALAXY_TALOS_CLIENT_ADMIN_TIMEOUT_MILLI_SECS_DEFAULT);
    serviceEndpoint = configuration.get(
        TalosClientConfigKeys.GALAXY_TALOS_SERVICE_ENDPOINT,
        TalosClientConfigKeys.GALAXY_TALOS_SERVICE_ENDPOINT_DEFAULT);
    secureServiceEndpoint = configuration.get(
        TalosClientConfigKeys.GALAXY_TALOS_SECURE_SERVICE_ENDPOINT,
        TalosClientConfigKeys.GALAXY_TALOS_SECURE_SERVICE_ENDPOINT_DEFAULT);
    maxTotalConnections = configuration.getInt(
        TalosClientConfigKeys.GALAXY_TALOS_HTTP_MAX_TOTAL_CONNECTION,
        TalosClientConfigKeys.GALAXY_TALOS_HTTP_MAX_TOTAL_CONNECTION_DEFAULT);
    maxTotalConnectionsPerRoute = configuration.getInt(
        TalosClientConfigKeys.GALAXY_TALOS_HTTP_MAX_TOTAL_CONNECTION_PER_ROUTE,
        TalosClientConfigKeys.GALAXY_TALOS_HTTP_MAX_TOTAL_CONNECTION_PER_ROUTE_DEFAULT);
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
