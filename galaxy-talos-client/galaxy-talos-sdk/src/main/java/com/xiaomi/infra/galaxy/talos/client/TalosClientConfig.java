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
  private String accessKey;
  private String accessSecret;
  private String topicName;
  private int partitionId;
  private int threadNumPerPartition;
  private int qpsPerThread;
  private String messageStr;
  private long putInterval;
  private int maxTotalConnections;
  private int maxTotalConnectionsPerRoute;
  private boolean isRetry;
  private boolean isAutoLocation;
  private int scheduleInfoMaxRetry;

  protected Properties properties;

  public TalosClientConfig() {
    initClientConfig(new Properties());
  }

  public TalosClientConfig(String fileName) {
    this(loadProperties(fileName));
  }

  public TalosClientConfig(Properties pro) {
    initClientConfig(pro);
    if (serviceEndpoint == null) {
      throw new RuntimeException(
          "The property of 'galaxy.talos.service.endpoint' must be set");
    }
  }

  private void initClientConfig(Properties pro) {
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
    accessKey = properties.getProperty(TalosClientConfigKeys.GALAXY_TALOS_ACCESSKEY, null);
    accessSecret = properties.getProperty(TalosClientConfigKeys.GALAXY_TALOS_ACCESSSECRET, null);
    topicName = properties.getProperty(TalosClientConfigKeys.GALAXY_TALOS_TOPICNAME, null);
    partitionId = Integer.parseInt(properties.getProperty(TalosClientConfigKeys.GALAXY_TALOS_PARTITIONID, "-1"));
    threadNumPerPartition = Integer.parseInt(properties.getProperty(
        TalosClientConfigKeys.THREAD_NUM_PERPARTITION, "1"));
    qpsPerThread = Integer.parseInt(properties.getProperty(TalosClientConfigKeys.QPS_PERTHREAD, "-1"));
    messageStr = properties.getProperty(TalosClientConfigKeys.MESSAGE_STR, TalosClientConfigKeys.MESSAGE_STR_DEFAULT);
    putInterval = Integer.parseInt(properties.getProperty(TalosClientConfigKeys.GALAXY_TALOS_PUTINTERVAL, "5000"));
    maxTotalConnections = Integer.parseInt(properties.getProperty(
        TalosClientConfigKeys.GALAXY_TALOS_HTTP_MAX_TOTAL_CONNECTION,
        String.valueOf(TalosClientConfigKeys.GALAXY_TALOS_HTTP_MAX_TOTAL_CONNECTION_DEFAULT)));
    maxTotalConnectionsPerRoute = Integer.parseInt(properties.getProperty(
        TalosClientConfigKeys.GALAXY_TALOS_HTTP_MAX_TOTAL_CONNECTION_PER_ROUTE,
        String.valueOf(TalosClientConfigKeys.GALAXY_TALOS_HTTP_MAX_TOTAL_CONNECTION_PER_ROUTE_DEFAULT)));
    isRetry = Boolean.parseBoolean(properties.getProperty(
        TalosClientConfigKeys.GALAXY_TALOS_CLIENT_IS_RETRY,
        String.valueOf(TalosClientConfigKeys.GALAXY_TALOS_CLIENT_IS_RETRY_DEFAULT)));
    isAutoLocation = Boolean.parseBoolean(properties.getProperty(
        TalosClientConfigKeys.GALAXY_TALOS_CLIENT_IS_AUTO_LOCATION,
        String.valueOf(TalosClientConfigKeys.GALAXY_TALOS_CLIENT_IS_AUTO_LOCATION_DEFAULT)));
    scheduleInfoMaxRetry = Integer.parseInt(properties.getProperty(
        TalosClientConfigKeys.GALAXY_TALOS_CLIENT_SCHEDULE_INFO_MAX_RETRY,
        String.valueOf(TalosClientConfigKeys.GALAXY_TALOS_CLIENT_SCHEDULE_INFO_MAX_RETRY_DEFAULT)));
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

  public String getAccessKey() {
    return accessKey;
  }

  public String getAccessSecret() {
    return accessSecret;
  }

  public String getTopicName() {
    return topicName;
  }

  public int getPartitionId() {
    return partitionId;
  }

  public int getThreadNumPerPartition() {
    return threadNumPerPartition;
  }

  public int getQpsPerThread() {
    return qpsPerThread;
  }

  public String getMessageStr() {
    return messageStr;
  }

  public long getPutInterval() {
    return putInterval;
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

  public boolean isAutoLocation() { return isAutoLocation; }

  public int getScheduleInfoMaxRetry() { return scheduleInfoMaxRetry; }

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

  public void setMaxRetry(int maxRetry) {
    this.maxRetry = maxRetry;
  }

  public void setClientTimeout(int clientTimeout) {
    this.clientTimeout = clientTimeout;
  }

  public void setClientConnTimeout(int clientConnTimeout) {
    this.clientConnTimeout = clientConnTimeout;
  }

  public void setAdminOperationTimeout(int adminOperationTimeout) {
    this.adminOperationTimeout = adminOperationTimeout;
  }

  public void setServiceEndpoint(String serviceEndpoint) {
    this.serviceEndpoint = serviceEndpoint;
  }

  public void setMaxTotalConnections(int maxTotalConnections) {
    this.maxTotalConnections = maxTotalConnections;
  }

  public void setMaxTotalConnectionsPerRoute(int maxTotalConnectionsPerRoute) {
    this.maxTotalConnectionsPerRoute = maxTotalConnectionsPerRoute;
  }

  public void setRetry(boolean isRetry) {
    this.isRetry = isRetry;
  }

  public void setAutoLocation(boolean isAutoLocation) {
    this.isAutoLocation = isAutoLocation;
  }

  public void setScheduleInfoMaxRetry(int ScheduleInfoMaxRetry) {
    this.scheduleInfoMaxRetry = ScheduleInfoMaxRetry;
  }

}
