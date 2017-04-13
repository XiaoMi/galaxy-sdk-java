package com.xiaomi.infra.galaxy.vision.client;

import com.xiaomi.infra.galaxy.ai.common.ConnectionConfig;

/**
 * Copyright 2017, Xiaomi.
 * All rights reserved.
 */
public class VisionConfig extends ConnectionConfig {
  public static final int MAX_REQUEST_IMAGE_SIZE = 5 * 1024 * 1024;
  public static final String FDS_URI_SCHEME = "fds";

  public VisionConfig() {}
  
  public VisionConfig(String endpoint) {
    super(endpoint);
  }
  
  public VisionConfig(String endpoint, boolean enableHttps) {
    super(endpoint, enableHttps);
  }
}
