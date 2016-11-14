/**
 * Copyright 2016, Xiaomi.
 * All rights reserved.
 * Author: xiajun@xiaomi.com
 */

package com.xiaomi.infra.galaxy.lcs.log.core.dummy;

import com.xiaomi.infra.galaxy.lcs.log.core.ILogger;

public class DummyLogger implements ILogger {
  @Override
  public void error(String message) {
    System.out.println(message);
  }

  @Override
  public void error(String message, Throwable t) {
    System.out.println(message + ", " + t.toString());
  }

  @Override
  public void info(String message) {
    System.out.println(message);
  }

  @Override
  public void info(String message, Throwable t) {
    System.out.println(message + ", " + t.toString());
  }

  @Override
  public void debug(String message) {
    System.out.println(message);
  }

  @Override
  public void debug(String message, Throwable t) {
    System.out.println(message + ", " + t.toString());
  }
}
