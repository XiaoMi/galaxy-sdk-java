package com.xiaomi.infra.galaxy.rpc.util.clock;

public interface Adjustable  {
  public void adjust(long currentEpoch);
}
