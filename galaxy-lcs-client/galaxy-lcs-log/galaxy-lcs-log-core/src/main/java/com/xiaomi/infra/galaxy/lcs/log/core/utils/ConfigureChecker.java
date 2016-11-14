/**
 * Copyright 2016, Xiaomi.
 * All rights reserved.
 * Author: xiajun@xiaomi.com
 */

package com.xiaomi.infra.galaxy.lcs.log.core.utils;

public class ConfigureChecker {
  public static void checkConfigureBlank(String name, String config) {
    if (config == null || config.trim().length() == 0) {
      throw new IllegalArgumentException("Please not set \"" + name + "\" as blank");
    }
  }

  public static void checkConfigureRange(String name, int config, int minValue, int maxValue) {
    if (config < minValue || config > maxValue) {
      throw new IllegalArgumentException("Please set \"" + name + "\" in " +
          "range [" + minValue + ", " + maxValue + "]");
    }
  }

  public static void checkConfigureRange(String name, long config, long minValue, long maxValue) {
    if (config < minValue || config > maxValue) {
      throw new IllegalArgumentException("Please set \"" + name + "\" in " +
          "range [" + minValue + ", " + maxValue + "]");
    }
  }
}
