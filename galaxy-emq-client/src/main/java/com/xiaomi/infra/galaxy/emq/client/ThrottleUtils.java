package com.xiaomi.infra.galaxy.emq.client;

import java.util.Random;

import com.xiaomi.infra.galaxy.emq.thrift.CommonConstants;
import com.xiaomi.infra.galaxy.emq.thrift.ErrorCode;

/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: qiankai@xiaomi.com
 */

public class ThrottleUtils {
  private static final Random RANDOM = new Random();

  /**
   * This is a retry backoff multiplier table similar to the BSD TCP syn
   * backoff table, a bit more aggressive than simple exponential backoff.
   */
  public static int RETRY_BACKOFF[] = {1, 4, 8, 16, 32, 64};

  /**
   * Calculate random time with 1% possible jitter
   *
   * @param normalPause
   * @return random time value
   */
  private static long getRandom(final long normalPause) {
    long jitter = (long) (normalPause * RANDOM.nextFloat() * 0.10f); // 10% possible jitter
    return normalPause + jitter;
  }

  /**
   * Calculate pause time.
   * Built on {@link #RETRY_BACKOFF}.
   *
   * @param pause
   * @param tries
   * @return How long to wait after <code>tries</code> retries
   */
  private static long getBackoffTime(final long pause, final int tries) {
    int ntries = tries;
    if (ntries >= RETRY_BACKOFF.length) {
      ntries = RETRY_BACKOFF.length - 1;
    }

    long normalPause = pause * RETRY_BACKOFF[ntries];
    return getRandom(normalPause);
  }

  /**
   * Calculate pause time based on multiplicative decrease back-off policy
   *
   * @param code
   * @param retry
   * @return time to pause
   */
  public static long getPauseTime(ErrorCode code, int retry) {
    Long time = CommonConstants.ERROR_BACKOFF.get(code);
    if (time == null) {
      return -1;
    }
    return getBackoffTime(time, retry);
  }

  /**
   * Calculate pause time based on additive increase back-off policy
   *
   * @return time to pause
   */
  public static long getPauseTime(long lastPauseTime) {
    if (lastPauseTime == 0) {
      return 0;
    }
    // decrease time with the larger of (the last pause time * 20%) and 200
    long pauseTime = lastPauseTime - getRandom(Math.max((long) (lastPauseTime * 0.2), 200));
    pauseTime = pauseTime < 0 ? 0 : pauseTime;
    return pauseTime;
  }

  public static void sleepPauseTime(long pauseTime) {
    if (pauseTime > 0) {
      try {
        Thread.sleep(pauseTime);
      } catch (InterruptedException ie) {
        throw new RuntimeException("thread sleep failed", ie);
      }
    }
  }
}