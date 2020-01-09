package com.xiaomi.infra.galaxy.emq.client;

/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: shenyuannan@xiaomi.com
 */

public class EMQConstants {
  /**
   * HTTP RPC服务地址
   */
  public static final String DEFAULT_SERVICE_ENDPOINT = "http://emq.api.xiaomi.com";

  /**
   * HTTPS RPC服务地址
   */
  public static final String DEFAULT_SECURE_SERVICE_ENDPOINT = "https://emq.api.xiaomi.com";

  /**
   * Queue操作RPC路径
   */
  public static final String QUEUE_SERVICE_PATH = "/v1/api/queue";

  /**
   * Message操作RPC路径
   */
  public static final String MESSAGE_SERVICE_PATH = "/v1/api/message";

  /**
   * Statistics操作RPC路径
   */
  public static final String STATISTICS_SERVICE_PATH = "/v1/api/statistics";
}
