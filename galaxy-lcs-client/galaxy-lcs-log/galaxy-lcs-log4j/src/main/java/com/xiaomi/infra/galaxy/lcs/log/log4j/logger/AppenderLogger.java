/**
 * Copyright 2016, Xiaomi.
 * All rights reserved.
 * Author: xiajun@xiaomi.com
 */

package com.xiaomi.infra.galaxy.lcs.log.log4j.logger;

import org.apache.log4j.Logger;

import com.xiaomi.infra.galaxy.lcs.log.core.ILogger;

public class AppenderLogger implements ILogger {
  private final Logger LOG;

  public AppenderLogger(Logger LOG) {
    this.LOG = LOG;
  }

  @Override
  public void error(String message) {
    LOG.error(message);
  }

  @Override
  public void error(String message, Throwable t) {
    LOG.error(message, t);
  }

  @Override
  public void info(String message) {
    LOG.info(message);
  }

  @Override
  public void info(String message, Throwable t) {
    LOG.info(message, t);
  }

  @Override
  public void debug(String message) {
    if (LOG.isDebugEnabled()) {
      LOG.debug(message);
    }
  }

  @Override
  public void debug(String message, Throwable t) {
    if (LOG.isDebugEnabled()) {
      LOG.debug(message, t);
    }
  }
}
