package com.xiaomi.infra.galaxy.metrics.client;

import com.xiaomi.infra.galaxy.metrics.thrift.ErrorCode;
import com.xiaomi.infra.galaxy.metrics.thrift.ErrorsConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: linshangquan@xiaomi.com
 */
public class MetricsAutoRetryClient {
  private static class AutoRetryHandler<T> implements InvocationHandler {
    private static final Logger LOG = LoggerFactory.getLogger(AutoRetryHandler.class);
    private final Object instance;
    private final boolean isRetry;
    private final int maxRetry;

    public AutoRetryHandler(Class<T> interfaceClass, Object instance, boolean isRetry,
        int maxRetry) {
      this.instance = instance;
      this.isRetry = isRetry;
      this.maxRetry = maxRetry;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      int retry = 0;
      do {
        try {
          return method.invoke(instance, args);
        } catch (InvocationTargetException e) {
          Throwable cause = e.getCause();
          if (isRetry && retry < maxRetry) {
            ErrorCode code = RetryUtils.getErrorCode(cause);
            Long sleepTime = ErrorsConstants.ERROR_BACKOFF.get(code);
            if (sleepTime != null) {
              retry++;
              LOG.warn("sleep for {} ms in the {} retry", sleepTime, retry);
              Thread.sleep(sleepTime);
            } else {
              throw cause;
            }
          } else {
            throw cause;
          }
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
    return (T) Proxy
        .newProxyInstance(MetricsAutoRetryClient.class.getClassLoader(), new Class[] { interfaceClass },
            new AutoRetryHandler<T>(interfaceClass, instance, isRetry, maxRetry));
  }
}
