package com.xiaomi.infra.galaxy.sds.client;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import com.xiaomi.infra.galaxy.sds.thrift.ServiceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.infra.galaxy.sds.thrift.ErrorCode;
import com.xiaomi.infra.galaxy.sds.thrift.ErrorsConstants;

/**
 * Auto retry client proxy.
 * @author heliangliang
 */
public class AutoRetryClient {
  private static class AutoRetryHandler<T> implements InvocationHandler {
    private static final Logger LOG = LoggerFactory.getLogger(AutoRetryHandler.class);
    private final Object instance;
    private final int maxRetry;

    public AutoRetryHandler(Class<T> interfaceClass, Object instance, int maxRetry) {
      this.instance = instance;
      this.maxRetry = maxRetry;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      int retry = 0;
      do {
        try {
          Object ret = method.invoke(instance, args);
          return ret;
        } catch (InvocationTargetException e) {
          Throwable cause = e.getCause();
          int backoff = backoffTime(cause);
          if (retry >= maxRetry || backoff < 0) {
            LOG.debug("Won't retry, retry = {}", retry);
            throw cause;
          }
          if (backoff > 0) {
            Thread.sleep(backoff << retry);
          }
          ++retry;
          LOG.debug("Auto retrying RPC call {} for {} time", method.getName(), retry);
        }
      } while (true);
    }

    /**
     * Backoff time
     * @return >= 0: do retry and backoff; < 0: no retry
     */
    private int backoffTime(Throwable cause) {
      ErrorCode code = ErrorCode.UNKNOWN;
      if (cause instanceof ServiceException) {
        ServiceException se = (ServiceException) cause;
        code = se.getErrorCode();
      } else if (cause instanceof HttpTTransportException) {
        HttpTTransportException te = (HttpTTransportException) cause;
        code = te.getErrorCode();
      }

      Integer time = ErrorsConstants.ERROR_AUTO_BACKOFF.get(code);
      LOG.debug("Base backoff time for error code {}: {} ms", code, time);
      return time == null ? -1 : time;
    }
  }

  /**
   * Create client wrapper which automatically retry the RPC calls for retryable errors until
   * success or reaches max retry time.
   */
  @SuppressWarnings("unchecked")
  public static <T> T getAutoRetryClient(Class<T> interfaceClass, Object instance, int maxRetry) {
    return (T) Proxy.newProxyInstance(AutoRetryClient.class.getClassLoader(),
      new Class[] { interfaceClass }, new AutoRetryHandler<T>(interfaceClass, instance, maxRetry));
  }
}
