/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: yongxing@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.client;

public class Constants {
  // TODO: merge common constants of client and server to thrift files
  /**
   * Constants for rest server path
   */
  public static final String TALOS_API_ROOT_PATH = "/v1/api";
  public static final String TALOS_TOPIC_SERVICE_PATH = TALOS_API_ROOT_PATH + "/topic";
  public static final String TALOS_MESSAGE_SERVICE_PATH = TALOS_API_ROOT_PATH + "/message";
  public static final String TALOS_QUOTA_SERVICE_PATH = TALOS_API_ROOT_PATH + "/quota";
  public static final String TALOS_CONSUMER_SERVICE_PATH = TALOS_API_ROOT_PATH + "/consumer";
  public static final String TALOS_METRIC_SERVICE_PATH = TALOS_API_ROOT_PATH + "/metric";

  public static final String TALOS_IDENTIFIER_DELIMITER = "#";
  public static final String TALOS_CONNECTION_DELIMITER = "-";
  public static final String TALOS_NAME_REGEX = "^(?!_)(?!-)(?!.*?_$)[a-zA-Z0-9_-]+$";
  public static final String TALOS_TEMPORARY_FILE_SUFFIX = ".tmp";

  /**
   * Constants for producer
   */
  public static final int TALOS_SINGLE_MESSAGE_BYTES_MINIMAL = 1;
  public static final int TALOS_SINGLE_MESSAGE_BYTES_MAXIMAL = 10 * 1024 * 1024;
  public static final int TALOS_MESSAGE_BLOCK_BYTES_MAXIMAL = 20 * 1024 * 1024;

  public static final int TALOS_PARTITION_KEY_LENGTH_MINIMAL = 1;
  public static final int TALOS_PARTITION_KEY_LENGTH_MAXIMAL = 256;

  /**
   * Constants for cloud-manager auth
   */
  public static final String TALOS_CLOUD_TOPIC_NAME_DELIMITER = "/";
  public static final String TALOS_CLOUD_ORG_PREFIX = "CL";
  public static final String TALOS_CLOUD_TEAM_PREFIX = "CI";
  public static final String TALOS_CLOUD_AK_PREFIX = "AK";
  public static final String TALOS_GALAXY_AK_PREFIX = "EAK";

  /**
   * Constants for consumer metrics
   */
  public static final String FETCH_MESSAGE_TIMES = "fetchMessage.60sRate";
  public static final String FETCH_MESSAGE_FAILED_TIMES = "fetchMessageFailed.60sRate";
  public static final String FETCH_MESSAGE_TIME = "fetchMessageTime.gauge";
  public static final String MAX_FETCH_MESSAGE_TIME = "fetchMessageTime.max";
  public static final String MIN_FETCH_MESSAGE_TIME = "fetchMessageTime.min";
  public static final String PROCESS_MESSAGE_TIME = "processMessageTime.gauge";
  public static final String MAX_PROCESS_MESSAGE_TIME = "processMessageTime.max";
  public static final String MIN_PROCESS_MESSAGE_TIME = "processMessageTime.min";

  /**
   * Constant for greedy consumer
   */
  public static final String CONSUMER_OFFSET_LAG = "consumerOffsetLag.60sRate";

  /**
   * Constants for producer metrics
   */
  public static final String PUT_MESSAGE_TIMES = "putMessage.60sRate";
  public static final String PUT_MESSAGE_FAILED_TIMES = "putMessageFailed.60sRate";
  public static final String PUT_MESSAGE_TIME = "putMessageTime.gauge";
  public static final String MAX_PUT_MESSAGE_TIME = "putMessageTime.max";
  public static final String MIN_PUT_MESSAGE_TIME = "putMessageTime.min";

}
