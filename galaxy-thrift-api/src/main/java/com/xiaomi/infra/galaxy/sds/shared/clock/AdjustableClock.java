package com.xiaomi.infra.galaxy.sds.shared.clock;

public class AdjustableClock implements Adjustable, Clock {
  private Clock localClock;
  private volatile long offset = 0;

  public AdjustableClock(Clock clock) {
    localClock = clock;
  }

  public AdjustableClock() {
    this(new LocalClock());
  }

  @Override
  public long getCurrentEpoch() {
    return localClock.getCurrentEpoch() + offset;
  }

  @Override
  public void adjust(long currentEpoch) {
    long localEpoch = localClock.getCurrentEpoch();
    offset = currentEpoch - localEpoch;
  }
}
