package com.xiaomi.infra.galaxy.sds.shared.clock;

public class LocalClock implements Clock {
  @Override
  public long getCurrentEpoch() {
    return System.currentTimeMillis() / 1000;
  }
}
