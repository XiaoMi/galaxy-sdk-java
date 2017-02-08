package com.xiaomi.infra.galaxy.metrics.example;

import com.xiaomi.infra.galaxy.metrics.client.MetricsClientFactory;
import com.xiaomi.infra.galaxy.metrics.thrift.Aggregator;
import com.xiaomi.infra.galaxy.metrics.thrift.AlertRule;
import com.xiaomi.infra.galaxy.metrics.thrift.CommonConstants;
import com.xiaomi.infra.galaxy.metrics.thrift.Credential;
import com.xiaomi.infra.galaxy.metrics.thrift.DownSample;
import com.xiaomi.infra.galaxy.metrics.thrift.JudgeService;
import com.xiaomi.infra.galaxy.metrics.thrift.Metric;
import com.xiaomi.infra.galaxy.metrics.thrift.MetricKey;
import com.xiaomi.infra.galaxy.metrics.thrift.MetricsService;
import com.xiaomi.infra.galaxy.metrics.thrift.QueryRequest;
import com.xiaomi.infra.galaxy.metrics.thrift.TimeUnit;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: linshangquan@xiaomi.com
 */
public class MetricsDemo {
  private static MetricsService.Iface metricsClient;
  private static JudgeService.Iface judgeClient;
  private static String appKey = ""; // Your AppKey
  private static String appSecret = ""; // Your AppSecret
  private static String endpoint = "";

  private static Credential getCredential(String secretKeyId, String secretKey) {
    return new Credential().setSecretKeyId(secretKeyId).setSecretKey(secretKey);
  }

  public static MetricsService.Iface createMetricsClient(String host) {
    Credential credential = getCredential(appKey, appSecret);
    MetricsClientFactory clientFactory = new MetricsClientFactory().setCredential(credential);
    return clientFactory.newMetricsClient(host + CommonConstants.METRICS_SERVICE_PATH, 50000, 3000);
  }

  public static JudgeService.Iface createJudgeClient(String host) {
    Credential credential = getCredential(appKey, appSecret);
    MetricsClientFactory clientFactory = new MetricsClientFactory().setCredential(credential);
    return clientFactory.newJudgeClient(host + CommonConstants.JUDGE_SERVICE_PATH, 50000, 3000);
  }

  public static void main(String[] args) throws Exception {
    metricsClient = createMetricsClient(endpoint);
    long now = System.currentTimeMillis() / 1000;
    String metricName = "test-app.availability";
    String [] hosts = {"host1", "host2", "host3"};
    for (long time = now; time < now + 20 * 60; time += 60) {
      for (String host : hosts) {
        Metric metric = new Metric()
            .setTimestamp(time)
            .setValue(100.0);
        MetricKey metricKey = new MetricKey()
            .setMetricName(metricName);
        metricKey.putToTags("host", host);
        metric.setMetricKey(metricKey);
        metricsClient.pushMetrics(Arrays.asList(metric));
      }
    }

    DownSample sample = new DownSample()
        .setInternal(1)
        .setTimeUnit(TimeUnit.MINUTE)
        .setAggregator(Aggregator.AVG);

    MetricKey metricKey = new MetricKey()
        .setMetricName(metricName);

    QueryRequest queryRequest = new QueryRequest()
        .setStartTime(now)
        .setEndTime(now + 20 * 60)
        .setMetricKey(metricKey)
        .setAggregator(Aggregator.AVG)
        .setDownSample(sample);
    System.out.println(metricsClient.queryMetric(queryRequest));
  }
}
