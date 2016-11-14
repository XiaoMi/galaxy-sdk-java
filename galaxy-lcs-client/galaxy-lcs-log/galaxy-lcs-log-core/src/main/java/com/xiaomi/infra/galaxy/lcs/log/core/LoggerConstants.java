/**
 * Copyright 2016, Xiaomi.
 * All rights reserved.
 * Author: xiajun@xiaomi.com
 */

package com.xiaomi.infra.galaxy.lcs.log.core;

public class LoggerConstants {
  // common logger constants
  public static final long DEFAULT_MAX_BUFFER_MESSAGE_BYTES = 10L * 1024 * 1024;
  public static final long DEFAULT_MAX_BUFFER_MESSAGE_NUMBER = 100000000L;
  public static final boolean DEFAULE_BLOCK_WHEN_BUFFER_FULL = false;
  public static final long DEFAULT_FLUSH_MESSAGE_BYTES = 100L * 1024;
  public static final long DEFAULT_FLUSH_MESSAGE_NUMBER = 100L;
  public static final long DEFAULT_FLUSH_MESSAGE_INTERVAL_MILLIS = 1000L;
  public static final long DEFAULT_PERIOD_PERIOD_INTERVAL_MILLIS = 60L * 1000;

  // file related logger constants
  public static final long DEFAULT_MAX_FILE_NUMBER = 100000L;
  public static final long DEFAULT_ROTATE_FILE_BYTES = 10L * 1024 * 1024;
  public static final long DEFAULT_ROTATE_FILE_INTERVAL_MILLIS = 60L * 1000;
  public static final String FILE_NAME_FORMAT = "%019d";
  public static final String TEMP_FILE_PATH = "temp";
  public static final String TEMP_FILE_NAME_SUFFIX = "_tmp";
}
