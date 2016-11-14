/**
 * Copyright 2016, Xiaomi.
 * All rights reserved.
 * Author: xiajun@xiaomi.com
 */

package com.xiaomi.infra.galaxy.lcs.log.core;

/**
 * ILog if used for LCSLogger to log message, that is for LCS user to add
 * message;
 */
public interface ILog {
  public void info(Object message);

  public void debug(Object message);
}