/**
 * Copyright 2016, Xiaomi.
 * All rights reserved.
 * Author: xiajun@xiaomi.com
 */

package com.xiaomi.infra.galaxy.lcs.log.core;

/**
 * ILogger is used for LCS it self to print logger;
 */
public interface ILogger {
  public void error(String message);

  public void error(String message, Throwable t);

  public void info(String message);

  public void info(String message, Throwable t);

  public void debug(String message);

  public void debug(String message, Throwable t);
}
