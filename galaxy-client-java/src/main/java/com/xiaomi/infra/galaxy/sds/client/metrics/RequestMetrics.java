package com.xiaomi.infra.galaxy.sds.client.metrics;


import com.google.common.base.Preconditions;
import com.xiaomi.infra.galaxy.sds.thrift.ClientMetricType;
import com.xiaomi.infra.galaxy.sds.thrift.ClientMetrics;
import com.xiaomi.infra.galaxy.sds.thrift.LatencyMetricType;
import com.xiaomi.infra.galaxy.sds.thrift.MetricData;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.HashMap;
import java.util.Map;



public class RequestMetrics {
  private static final Log LOG = LogFactory.getLog(RequestMetrics.class);
  private String queryString;
  private Map<LatencyMetricType, TimingInfo> latencyMetrics
      = new HashMap<LatencyMetricType, TimingInfo>();

  public void setRequestTypeName(String queryString) {
    this.queryString = queryString;
  }

  public void startEvent(LatencyMetricType metricType) {
    TimingInfo timingInfo = new TimingInfo(System.currentTimeMillis(), null);
    latencyMetrics.put(metricType, timingInfo);
  }

  public void endEvent(LatencyMetricType metricType) {
    TimingInfo timingInfo = latencyMetrics.get(metricType);
    if (timingInfo == null) {
      LOG.warn("Try to end event which wasn't started.");
      return;
    }
    timingInfo.setEndTimeMilli(System.currentTimeMillis());
  }

  public ClientMetrics toClientMetrics() {
    ClientMetrics clientMetrics = new ClientMetrics();

    for (Map.Entry<LatencyMetricType, TimingInfo> entry
        : latencyMetrics.entrySet()) {
      TimingInfo timingInfo = entry.getValue();
      Preconditions.checkNotNull(timingInfo.getStartTimeMilli());
      Preconditions.checkNotNull(timingInfo.getEndTimeMilli());
      String metricName = queryString + "." + entry.getKey().toString();
      MetricData metricData = new MetricData()
          .setClientMetricType(ClientMetricType.Letency)
          .setMetricName(metricName)
          .setValue(timingInfo.getEndTimeMilli() - timingInfo.getStartTimeMilli())
          .setTimeStamp(timingInfo.getEndTimeMilli() / 1000);
      clientMetrics.addToMetricDataList(metricData);
    }

    return clientMetrics;
  }

  private class TimingInfo {
    private Long startTimeMilli;
    private Long endTimeMilli;

    public TimingInfo(Long startTimeMilli, Long endTimeMilli) {
      this.startTimeMilli = startTimeMilli;
      this.endTimeMilli = endTimeMilli;
    }

    public Long getStartTimeMilli() {
      return startTimeMilli;
    }

    public void setStartTimeMilli(Long startTimeMilli) {
      this.startTimeMilli = startTimeMilli;
    }

    public Long getEndTimeMilli() {
      return endTimeMilli;
    }

    public void setEndTimeMilli(Long endTimeMilli) {
      this.endTimeMilli = endTimeMilli;
    }
  }
}
