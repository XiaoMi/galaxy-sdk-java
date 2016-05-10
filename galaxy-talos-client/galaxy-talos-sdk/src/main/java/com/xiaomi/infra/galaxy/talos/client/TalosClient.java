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

public class TalosClient {

  private static class TalosClientHandler<T> implements InvocationHandler {
    private final Object instance;

    public TalosClientHandler(Class<T> interfaceClass, Object instance) {
      this.instance = instance;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args)
        throws Throwable {
      try {
        return method.invoke(instance, args);
      } catch (InvocationTargetException e) {
        throw e.getCause();
      }
    }
  }

  @SuppressWarnings("unchecked")
  public static <T> T getClient(Class<T> interfaceClass, Object instance) {
    return (T) Proxy.newProxyInstance(TalosClient.class.getClassLoader(),
        new Class[]{ interfaceClass },
        new TalosClientHandler(interfaceClass, instance));
  }
}
