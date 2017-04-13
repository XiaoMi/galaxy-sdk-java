package com.xiaomi.infra.galaxy.ai.common;

/**
 * Copyright 2017, Xiaomi.
 * All rights reserved.
 */
public class ConnectionConfig {
  private static final String URI_HTTP_PREFIX = "http://";
  private static final String URI_HTTPS_PREFIX = "https://";
  
  /**
   * The default timeout for a connected socket.
   */
  public static final int DEFAULT_SOCKET_TIMEOUT_MS = 50 * 1000;

  /**
   * The default timeout for establishing a connection.
   */
  public static final int DEFAULT_CONNECTION_TIMEOUT_MS = 50 * 1000;

  /**
   * max connections a client can have at same time
   */
  private static final int DEFAULT_MAX_CONNECTIONS = 20;

  /**
   * retry number for service unavailable status code
   */
  public static final int DEFAULT_RETRY_COUNT = 3;

  private int connectionTimeoutMs = DEFAULT_CONNECTION_TIMEOUT_MS;
  private int socketTimeoutMs = DEFAULT_SOCKET_TIMEOUT_MS;
  private int maxConnection = DEFAULT_MAX_CONNECTIONS;
  private int retryCount = DEFAULT_RETRY_COUNT;
  private String endpoint;
  private boolean enableHttps;
  
  public ConnectionConfig() {}
  
  public ConnectionConfig(String endpoint) {
    this(endpoint, false);
  }
  
  public ConnectionConfig(String endpoint, boolean enableHttps) {
    this.endpoint = endpoint;
    this.enableHttps = enableHttps;
  }
  
  public int getConnectionTimeoutMs() {
    return connectionTimeoutMs;
  }

  public void setConnectionTimeoutMs(int connectionTimeoutMs) {
    this.connectionTimeoutMs = connectionTimeoutMs;
  }

  public int getSocketTimeoutMs() {
    return socketTimeoutMs;
  }

  public void setSocketTimeoutMs(int socketTimeoutMs) {
    this.socketTimeoutMs = socketTimeoutMs;
  }

  public int getRetryCount() {
    return this.retryCount;
  }

  public void setRetryCount(int retryCount) {
    this.retryCount = retryCount;
  }

  public int getMaxConnection() {
    return maxConnection;
  }

  public void setMaxConnection(int maxConnection) {
    this.maxConnection = maxConnection;
  }

  public String getEndpoint() {
    return endpoint;
  }

  public void setEndpoint(String endpoint) {
    this.endpoint = endpoint;
  }
  
  public boolean isHttpsEnabled() {
    return enableHttps;
  }

  public void enableHttps(boolean enableHttps) {
    this.enableHttps = enableHttps;
  }
  
  public String getBaseUri() {
    StringBuilder sb = new StringBuilder(enableHttps ? URI_HTTPS_PREFIX : URI_HTTP_PREFIX);
    sb.append(endpoint);
    sb.append("/");
    return sb.toString();
  }
}
