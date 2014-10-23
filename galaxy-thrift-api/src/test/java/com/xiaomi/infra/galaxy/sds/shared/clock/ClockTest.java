package com.xiaomi.infra.galaxy.sds.shared.clock;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class ClockTest {
  @Test
  public void testClocks() throws Exception {
    Clock baseline = new LocalClock();
    AdjustableClock real = new AdjustableClock(new SkewedClock(baseline));

    int skewed = 0;
    for (int i = 0; i < 100000; ++i) {
      long t1 = baseline.getCurrentEpoch();
      long t2 = real.getCurrentEpoch();
      if (Math.abs(t1 - t2) >= 60) {
        skewed++;
        real.adjust(t1);
        t2 = real.getCurrentEpoch();
      }
      assertTrue("clock adjusting failed: " + t1 + " != " + t2, Math.abs(t1 - t2) < 60);
    }
    assertTrue(skewed > 0);
  }
}
