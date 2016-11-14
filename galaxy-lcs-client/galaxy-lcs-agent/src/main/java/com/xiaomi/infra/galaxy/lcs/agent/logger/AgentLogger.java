/**
 * Copyright 2016, Xiaomi.
 * All rights reserved.
 * Author: xiajun@xiaomi.com
 */

package com.xiaomi.infra.galaxy.lcs.agent.logger;

import org.slf4j.Logger;

import com.xiaomi.infra.galaxy.lcs.log.core.ILogger;

public class AgentLogger implements ILogger {
  private final Logger logger;
  public AgentLogger(Logger logger) {
    this.logger = logger;
  }

  @Override
  public void error(String message) {
    logger.error(message);
  }

  @Override
  public void error(String message, Throwable t) {
    logger.error(message, t);
  }

  @Override
  public void info(String message) {
    logger.info(message);
  }

  @Override
  public void info(String message, Throwable t) {
    logger.info(message, t);
  }

  @Override
  public void debug(String message) {
    logger.debug(message);
  }

  @Override
  public void debug(String message, Throwable t) {
    logger.debug(message, t);
  }
}
