/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: yongxing@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.client;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TalosClientConfig implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(TalosClientConfig.class);
  private int maxRetry;
  private int clientTimeout;
  private int clientConnTimeout;
  private int adminOperationTimeout;
  private String serviceEndpoint;
  private int maxTotalConnections;
  private int maxTotalConnectionsPerRoute;
  private boolean isRetry;

  protected Properties properties;

  public TalosClientConfig(String fileName) {
    this(loadProperties(fileName));
  }

  public TalosClientConfig(Properties pro) {
    this.properties = pro;

    maxRetry = Integer.parseInt(properties.getProperty(
        TalosClientConfigKeys.GALAXY_TALOS_CLIENT_MAX_RETRY, String.valueOf(
            TalosClientConfigKeys.GALAXY_TALOS_CLIENT_MAX_RETRY_DEFAULT)));
    clientTimeout = Integer.parseInt(properties.getProperty(
        TalosClientConfigKeys.GALAXY_TALOS_CLIENT_TIMEOUT_MILLI_SECS, String.valueOf(
            TalosClientConfigKeys.GALAXY_TALOS_CLIENT_TIMEOUT_MILLI_SECS_DEFAULT)));
    clientConnTimeout = Integer.parseInt(properties.getProperty(
        TalosClientConfigKeys.GALAXY_TALOS_CLIENT_CONN_TIMECOUT_MILLI_SECS,
        String.valueOf(TalosClientConfigKeys.GALAXY_TALOS_CLIENT_CONN_TIMECOUT_MILLI_SECS_DEFAULT)));
    adminOperationTimeout = Integer.parseInt(properties.getProperty(
        TalosClientConfigKeys.GALAXY_TALOS_CLIENT_ADMIN_TIMEOUT_MILLI_SECS,
        String.valueOf(TalosClientConfigKeys.GALAXY_TALOS_CLIENT_ADMIN_TIMEOUT_MILLI_SECS_DEFAULT)));
    serviceEndpoint = properties.getProperty(
        TalosClientConfigKeys.GALAXY_TALOS_SERVICE_ENDPOINT, null);

    if (serviceEndpoint == null) {
      throw new RuntimeException(
          "The property of 'galaxy.talos.service.endpoint' must be set");
    }

    maxTotalConnections = Integer.parseInt(properties.getProperty(
        TalosClientConfigKeys.GALAXY_TALOS_HTTP_MAX_TOTAL_CONNECTION,
        String.valueOf(TalosClientConfigKeys.GALAXY_TALOS_HTTP_MAX_TOTAL_CONNECTION_DEFAULT)));
    maxTotalConnectionsPerRoute = Integer.parseInt(properties.getProperty(
        TalosClientConfigKeys.GALAXY_TALOS_HTTP_MAX_TOTAL_CONNECTION_PER_ROUTE,
        String.valueOf(TalosClientConfigKeys.GALAXY_TALOS_HTTP_MAX_TOTAL_CONNECTION_PER_ROUTE_DEFAULT)));
    isRetry = Boolean.parseBoolean(properties.getProperty(
        TalosClientConfigKeys.GALAXY_TALOS_CLIENT_IS_RETRY,
        String.valueOf(TalosClientConfigKeys.GALAXY_TALOS_CLIENT_IS_RETRY_DEFAULT)));
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

  public int getMaxTotalConnections() {
    return maxTotalConnections;
  }

  public int getMaxTotalConnectionsPerRoute() {
    return maxTotalConnectionsPerRoute;
  }

  public boolean isRetry() {
    return isRetry;
  }

  public static Properties loadProperties(String fileName) {
    Properties properties = new Properties();
    FileInputStream inputStream = null;
    try {
      inputStream = new FileInputStream(fileName);
    } catch (FileNotFoundException e) {
      throw new RuntimeException("Load properties file error for: " + fileName, e);
    }

    try {
      properties.load(inputStream);
    } catch (IOException e) {
      LOG.warn("Load FileInputStream exception", e);
    }

    return properties;
  }
}
