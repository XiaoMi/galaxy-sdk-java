/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 * Author: yongxing@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.client;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.http.conn.DnsResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TalosDNSResolver implements DnsResolver {
  private static final Logger LOG = LoggerFactory.getLogger(TalosDNSResolver.class);

  private final ExecutorService executor = Executors.newSingleThreadExecutor();
  private long timeout;

  public TalosDNSResolver(long timeout) {
    this.timeout = timeout;
  }

  @Override
  public InetAddress[] resolve(final String hostName) throws UnknownHostException {
    Future<InetAddress[]> future = executor.submit(new ResolveCallable(hostName));

    try {
      return future.get(timeout, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      LOG.warn("TalosDNSResolver error: " + e.toString(), e);
      throw new UnknownHostException("TalosDNSResolver error: " + e.toString());
    } finally {
      future.cancel(true);
    }
  }

  private class ResolveCallable implements Callable<InetAddress[]> {
    private String hostName;
    private ResolveCallable(String hostName) {
      this.hostName = hostName;
    }

    @Override
    public InetAddress[] call() throws Exception {
      return InetAddress.getAllByName(hostName);
    }
  }
}


