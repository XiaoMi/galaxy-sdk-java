package com.xiaomi.infra.galaxy.sds.shared.clock;

import java.util.Random;

public class SkewedClock implements Clock {
  private Clock baseClock;
  private int count = 0;
  private int offset = 0;
  private Random rand = new Random();

  public SkewedClock(Clock clock) {
    baseClock = clock;
  }

  public SkewedClock() {
    this(new LocalClock());
  }

  @Override
  public synchronized long getCurrentEpoch() {
    if (count++ % 5 == 0) {
      int month = 60 * 60 * 24 * 30;
      offset = rand.nextInt(2 * month) - month; // [-1 month, 1 month]
    }
    return baseClock.getCurrentEpoch() + offset;
  }
}
