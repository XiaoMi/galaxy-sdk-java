/**
 * Copyright 2016, Xiaomi.
 * All rights reserved.
 * Author: xiajun@xiaomi.com
 */

package com.xiaomi.infra.galaxy.lcs.log.log4j;

import org.apache.log4j.Logger;

import com.xiaomi.infra.galaxy.lcs.log.core.ILog;
import com.xiaomi.infra.galaxy.lcs.log.core.LCSLoggerBase;

public class LCSLogger extends LCSLoggerBase {
  private static final Logger LOG = Logger.getLogger(LCSLogger.class);

  public LCSLogger() {
    super(new ILog() {
      @Override
      public void info(Object message) {
        LOG.info(message);
      }

      @Override
      public void debug(Object message) {
        LOG.debug(message);
      }
    });
  }
}
