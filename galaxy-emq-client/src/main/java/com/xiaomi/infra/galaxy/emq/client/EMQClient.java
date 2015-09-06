package com.xiaomi.infra.galaxy.emq.client;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: shenyuannan@xiaomi.com
 */

public class EMQClient {
  private static class EMQClientHandler<T> implements InvocationHandler {
    private final Object instance;

    public EMQClientHandler(Class<T> interfaceClass, Object instance) {
      this.instance = instance;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args)
        throws Throwable {
      EMQRequestCheckUtils.checkRequest(args);
      try {
        return method.invoke(instance, args);
      } catch (InvocationTargetException e) {
        throw e.getCause();
      }
    }
  }

  /**
   * Create client wrapper which validate parameters of client.
   */
  @SuppressWarnings("unchecked")
  public static <T> T getClient(Class<T> interfaceClass, Object instance) {
    return (T) Proxy.newProxyInstance(EMQClient.class.getClassLoader(),
        new Class[]{interfaceClass},
        new EMQClientHandler(interfaceClass, instance));
  }
}
