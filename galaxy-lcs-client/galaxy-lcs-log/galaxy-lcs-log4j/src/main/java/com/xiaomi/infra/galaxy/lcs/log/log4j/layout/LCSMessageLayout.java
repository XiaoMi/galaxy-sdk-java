/**
 * Copyright 2016, Xiaomi.
 * All rights reserved.
 * Author: xiajun@xiaomi.com
 */

package com.xiaomi.infra.galaxy.lcs.log.log4j.layout;

import org.apache.log4j.Layout;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;

public class LCSMessageLayout extends Layout {
  private static final Logger LOG = Logger.getLogger(LCSMessageLayout.class);
  @Override
  public String format(LoggingEvent loggingEvent) {
    throw new RuntimeException("We should never call LCSMessageLayout::format");
  }

  @Override
  public boolean ignoresThrowable() {
    return false;
  }

  @Override
  public void activateOptions() {

  }
}
