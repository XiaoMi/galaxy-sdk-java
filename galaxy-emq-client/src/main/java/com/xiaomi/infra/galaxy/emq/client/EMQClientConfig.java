package com.xiaomi.infra.galaxy.emq.client;

import java.util.Properties;

/**
 * Copyright 2020, Xiaomi.
 * All rights reserved.
 * Author: shanzhongkai@xiaomi.com
 */
public class EMQClientConfig {
  private String endpoint;
  private int socketTimeout;
  private int connTimeout;
  private boolean isRetry;
  private int maxRetry;

  public EMQClientConfig(Properties properties) {
    loadConfig(properties);
  }

  public void loadConfig(Properties properties) {
    this.endpoint = properties.getProperty(EMQClientConfigKeys.GALAXY_EMQ_SERVICE_ENDPOINT,
        EMQClientConfigKeys.GALAXY_EMQ_SERVICE_ENDPOINT_DEFAULT);
    this.socketTimeout = Integer.parseInt(properties.getProperty(EMQClientConfigKeys.GALAXY_EMQ_CLIENT_TIMEOUT,
        String.valueOf(EMQClientConfigKeys.GALAXY_EMQ_CLIENT_TIMEOUT_DEFAULT)));
    this.connTimeout = Integer.parseInt(properties.getProperty(EMQClientConfigKeys.GALAXY_EMQ_CLIENT_CONN_TIMEOUT,
        String.valueOf(EMQClientConfigKeys.GALAXY_EMQ_CLIENT_CONN_TIMEOUT_DEFAULT)));
    this.isRetry = Boolean.parseBoolean(properties.getProperty(EMQClientConfigKeys.GALAXY_EMQ_CLIENT_AUTO_RETRY,
        String.valueOf(EMQClientConfigKeys.GALAXY_EMQ_CLIENT_AUTO_RETRY_DEFAULT)));
    this.maxRetry = Integer.parseInt(properties.getProperty(EMQClientConfigKeys.GALAXY_EMQ_CLIENT_RETRY_NUMBER,
        String.valueOf(EMQClientConfigKeys.GALAXY_EMQ_CLIENT_RETRY_NUMBER_DEFAULT)));
  }

  public String getEndpoint() {
    return endpoint;
  }

  public int getSocketTimeout() {
    return socketTimeout;
  }

  public int getConnTimeout() {
    return connTimeout;
  }

  public boolean getIsRetry() {
    return isRetry;
  }

  public int getMaxRetry() {
    return maxRetry;
  }
}
