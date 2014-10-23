package com.xiaomi.infra.galaxy.sds.shared.clock;

public interface Adjustable  {
  public void adjust(long currentEpoch);
}
