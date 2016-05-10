/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: yongxing@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.client;

import org.apache.hadoop.conf.Configuration;

public class TalosClientConfigurationLoader {
  private static Configuration INSTANCE = null;

  public static Configuration getConfiguration() {
    if (INSTANCE != null) {
      return INSTANCE;
    }

    INSTANCE = new Configuration();
    return INSTANCE;
  }
}
