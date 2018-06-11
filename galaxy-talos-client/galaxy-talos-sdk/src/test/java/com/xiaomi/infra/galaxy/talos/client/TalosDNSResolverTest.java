/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 * Author: yongxing@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.client;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TalosDNSResolverTest {
  public static final Logger LOG = LoggerFactory.getLogger(TalosDNSResolverTest.class);
  private static final long shortTimeout = 0;
  private static final long longTimeout = 100;
  private static TalosDNSResolver dnsResolverShort;
  private static TalosDNSResolver dnsResolverLong;

  @BeforeClass
  public static void setUp() {
    dnsResolverShort = new TalosDNSResolver(shortTimeout);
    dnsResolverLong = new TalosDNSResolver(longTimeout);
  }

  @AfterClass
  public static void tearDown() {

  }

  @Test
  public void testDNSResolver() {
    try {
      InetAddress[] res = dnsResolverShort.resolve("www.baidu.com");
      assertTrue("test baidu.com error", false);
    } catch (UnknownHostException e) {
      System.out.println("test baidu.com err" + e.toString());
    }

    try {
      InetAddress[] res = dnsResolverShort.resolve("mi.com");
      assertTrue("test mi.com error", false);
    } catch (UnknownHostException e) {
      System.out.println("test mi.com err" + e.toString());
    }

    try {
      InetAddress[] res = dnsResolverLong.resolve("www.baidu.com");
      assertTrue("test baidu.com error", res.length > 0);
      System.out.println("test baidu.com: " + res[0]);
    } catch (UnknownHostException e) {
      assertFalse("test baidu.com err" + e.toString(), true);
    }

    try {
      InetAddress[] res = dnsResolverLong.resolve("mi.com");
      assertTrue("test mi.com error", res.length > 0);
      System.out.println("test mi.com: " + res[0]);
    } catch (UnknownHostException e) {
      assertFalse("test mi.com err" + e.toString(), true);
    }

    try {
      InetAddress[] res = dnsResolverLong.resolve("unknowhelloyx.com");
      assertTrue("test unknow.com error " + res[0], false);
    } catch (UnknownHostException e) {
      System.out.println("test unknow.com err" + e.toString());
    }
  }
}
