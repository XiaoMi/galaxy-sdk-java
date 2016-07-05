/**
 * Copyright 2016, Xiaomi.
 * All rights reserved.
 * Author: xiajun@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.mapreduce.input.config;

import org.apache.hadoop.conf.Configuration;

public class TalosTopicInputConfiguration {
  public static final String GALAXY_TALOS_MAPREDUCE_SERVICE_ENDPOINT =
      "galaxy.talos.mapreduce.service.endpoint";

  /**
   * The topic resource name to consume;
   */
  public static final String GALAXY_TALOS_MAPREDUCE_TOPIC_RESOURCE_NAME =
      "galaxy.talos.mapreduce.topic.resource.name";
  /**
   * The partition offset info, it must in format list below:
   * "partition1:startMessageOffset:endMessageOffset,partition2:startMessageOffset:endMessageOffset"
   */
  public static final String GALAXY_TALOS_MAPREDUCE_PARTITION_OFFSET =
      "galaxy.talos.maprduce.partition.offset";

  /**
   * The below is used for setup credential for talos topic, for more
   * information please visit http://docs.api.xiaomi.com/talos/api/auth.html
   */
  public static final String GALAXY_TALOS_MAPREDUCE_SECRET_ID =
      "galaxy.talos.mapreduce.secret.id";
  public static final String GALAXY_TALOS_MAPREDUCE_SECRET_KEY =
      "galaxy.talos.mapreduce.secret.key";
  /**
   * Only be "DEV_XIAOMI" or "APP_SECRET";
   */
  public static final String GALAXY_TALOS_MAPREDUCE_USER_TYPE =
      "galaxy.talos.mapreduce.user.type";

  public static final String GALAXY_TALOS_MAPREDUCE_CONSUME_DATA_MAX_RETRYS_BEFORE_FAIL_MAP_TASK =
      "galaxy.talos.mapreduce.consume.data.max.retrys.before.fail.map.task";

  public String talosEndpoint;
  public String topicResourceName;
  public String partitionOffset;
  public String secretId;
  public String secretKey;
  public String userType;
  public int maxRetrys;

  public TalosTopicInputConfiguration(Configuration configuration) {
    configuration.addResource("conf/talos-mr-site.xml");

    talosEndpoint = configuration.get(GALAXY_TALOS_MAPREDUCE_SERVICE_ENDPOINT, null);
    topicResourceName = configuration.get(GALAXY_TALOS_MAPREDUCE_TOPIC_RESOURCE_NAME, null);
    partitionOffset = configuration.get(GALAXY_TALOS_MAPREDUCE_PARTITION_OFFSET, null);
    secretId = configuration.get(GALAXY_TALOS_MAPREDUCE_SECRET_ID, null);
    secretKey = configuration.get(GALAXY_TALOS_MAPREDUCE_SECRET_KEY, null);
    userType = configuration.get(GALAXY_TALOS_MAPREDUCE_USER_TYPE, null);
    maxRetrys = configuration.getInt(GALAXY_TALOS_MAPREDUCE_CONSUME_DATA_MAX_RETRYS_BEFORE_FAIL_MAP_TASK, 5);

    if (talosEndpoint == null) {
      throw new IllegalArgumentException("please set " +
          "\"galaxy.talos.mapreduce.service.endpoint\"");
    }

    if (topicResourceName == null) {
      throw new IllegalArgumentException("please set " +
          "\"galaxy.talos.mapreduce.topic.resource.name\"");
    }
    if (topicResourceName.split("#").length != 3) {
      throw new IllegalArgumentException("please set " +
          "\"galaxy.talos.mapreduce.topic.resource.name\" as TopicTalosResourceName");
    }

    if (partitionOffset == null) {
      throw new IllegalArgumentException("please set " +
          "\"galaxy.talos.maprduce.partition.offset\"");
    }

    if (secretId == null) {
      throw new IllegalArgumentException("please set " +
          "\"galaxy.talos.mapreduce.secret.id\"");
    }

    if (secretKey == null) {
      throw new IllegalArgumentException("please set " +
          "\"galaxy.talos.mapreduce.secret.key\"");
    }

    if (userType == null) {
      throw new IllegalArgumentException("please set " +
          "\"galaxy.talos.mapreduce.user.type\"");
    }

    if (!userType.equals("DEV_XIAOMI") && !userType.equals("APP_SECRET")) {
      throw new IllegalArgumentException("\"galaxy.talos.mapreduce.user.type\" " +
          "must be \"DEV_XIAOMI\" or \"\"");
    }
  }

  public String getTalosEndpoint() {
    return talosEndpoint;
  }

  public void setTalosEndpoint(String talosEndpoint) {
    this.talosEndpoint = talosEndpoint;
  }

  public String getTopicResourceName() {
    return topicResourceName;
  }

  public void setTopicResourceName(String topicResourceName) {
    this.topicResourceName = topicResourceName;
  }

  public String getPartitionOffset() {
    return partitionOffset;
  }

  public void setPartitionOffset(String partitionOffset) {
    this.partitionOffset = partitionOffset;
  }

  public String getSecretId() {
    return secretId;
  }

  public void setSecretId(String secretId) {
    this.secretId = secretId;
  }

  public String getSecretKey() {
    return secretKey;
  }

  public void setSecretKey(String secretKey) {
    this.secretKey = secretKey;
  }

  public String getUserType() {
    return userType;
  }

  public void setUserType(String userType) {
    this.userType = userType;
  }

  public int getMaxRetrys() {
    return maxRetrys;
  }
}
