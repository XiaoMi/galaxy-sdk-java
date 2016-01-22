/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: yongxing@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.client;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.infra.galaxy.talos.thrift.ErrorCode;
import com.xiaomi.infra.galaxy.talos.thrift.RetryType;

public class AutoRetryClient {
  private static class AutoRetryHandler<T> implements InvocationHandler {
    private static final Logger LOG = LoggerFactory.getLogger(AutoRetryHandler.class);
    private final Object instance;
    private final boolean isRetry;
    private final int maxRetry;
    private ThreadLocal<Long> lastPauseTime = new ThreadLocal<Long>() {
      public Long initialValue() {
        return 0l;
      }
    };

    public AutoRetryHandler(Class<T> interfaceClass, Object instance, boolean isRetry,
        int maxRetry) {
      this.instance = instance;
      this.isRetry = isRetry;
      this.maxRetry = maxRetry;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      int retry = 0;
      long pauseTime = ThrottleUtils.getPauseTime(lastPauseTime.get());
      do {
        try {
          // sleep when entering a new method invoke
          if (retry == 0) {
            ThrottleUtils.sleepPauseTime(pauseTime);
          }
          Object ret = method.invoke(instance, args);
          lastPauseTime.set(pauseTime < 0 ? 0 : pauseTime);
          return ret;
        } catch (InvocationTargetException e) {
          Throwable cause = e.getCause();
          if (maxRetry < 0 || retry >= maxRetry) {
            lastPauseTime.set(pauseTime < 0 ? 0 : pauseTime);
            LOG.debug("reach max retry number, retry = {}", retry);
            throw cause;
          }

          ErrorCode code = RetryUtils.getErrorCode(cause);
          RetryType retryType = RetryUtils.getRetryType(code);
          pauseTime = ThrottleUtils.getPauseTime(code, retry);
          if (!(isRetry || (retryType != null && retryType.equals(RetryType.SAFE)))
              || pauseTime < 0) {
            lastPauseTime.set(pauseTime < 0 ? 0 : pauseTime);
            LOG.debug("Won't retry, retry = {}", retry);
            throw cause;
          }

          if (pauseTime >= 0) {
            ThrottleUtils.sleepPauseTime(pauseTime);
            LOG.debug("sleep for {} ms in the {} retry", pauseTime, retry);
          }
          ++retry;
          LOG.debug("Auto retrying RPC call {} for {} time", method.getName(), retry);
        }
      } while (true);
    }

  }

  /**
   * Create client wrapper which automatically retry the RPC calls for retryable errors until
   * success or reaches max retry time.
   */
  @SuppressWarnings("unchecked")
  public static <T> T getAutoRetryClient(Class<T> interfaceClass, Object instance, boolean isRetry,
      int maxRetry) {
    return (T) Proxy.newProxyInstance(AutoRetryClient.class.getClassLoader(),
        new Class[] { interfaceClass },
        new AutoRetryHandler<T>(interfaceClass, instance, isRetry, maxRetry));
  }
}
