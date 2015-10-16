package com.xiaomi.infra.galaxy.emr.client;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.xiaomi.infra.galaxy.emr.thrift.EMRAdminService;
import com.xiaomi.infra.galaxy.emr.thrift.Mesurement;
import com.xiaomi.infra.galaxy.emr.thrift.Metric;
import com.xiaomi.infra.galaxy.emr.thrift.MetricQueryRequest;
import com.xiaomi.infra.galaxy.rpc.thrift.Credential;

/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: liupengcheng@xiaomi.com
 */
public class TestEMRAdminClient {

  private EMRClientFactory clientFactory;
  private EMRAdminService.Iface client;
  private String endpoint = "http://lg-hadoop-open1-tst-emr01.bj:19400";
  private static String secretId = "5911733121553"; // your secretId
  private static String secretKey = "5i9ODNhs7X85JNf3t+rpHg=="; // your secretKey
  @Before
  public void init() {
    Credential credential = new Credential().setSecretKeyId(secretId)
        .setSecretKey(secretKey);
    clientFactory = new EMRClientFactory(credential);
    client = clientFactory.newEMRAdminService(endpoint);
  }
  @Test
  public void testQueryMetric() {
    MetricQueryRequest request = new MetricQueryRequest();
    long curTime = System.currentTimeMillis();
    request.setStartTime(curTime - 2 * 24 * 60 * 60 * 1000)
        .setEndTime(curTime)
        .setHost("lg-hadoop-open1-tst-emr01.bj")
        .setMetric(Metric.REQUEST_TOTAL)
        .setMesurement(Mesurement.M1_RATE)
        .setCalcRate(false);
    try {
      client.queryMetric(request);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
