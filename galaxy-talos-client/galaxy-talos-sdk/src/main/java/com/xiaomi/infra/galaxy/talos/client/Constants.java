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

  public static final String TALOS_IDENTIFIER_DELIMITER = "#";
  public static final String TALOS_NAME_REGEX = "^(?!_)(?!.*?_$)[a-zA-Z0-9_-]+$";

  /**
   * Constants for producer
   */
  public static final int TALOS_SINGLE_MESSAGE_BYTES_MINIMAL = 1;
  public static final int TALOS_SINGLE_MESSAGE_BYTES_MAXIMAL = 10 * 1024 * 1024;

  public static final int TALOS_PARTITION_KEY_LENGTH_MINIMAL = 1;
  public static final int TALOS_PARTITION_KEY_LENGTH_MAXIMAL = 256;

}
