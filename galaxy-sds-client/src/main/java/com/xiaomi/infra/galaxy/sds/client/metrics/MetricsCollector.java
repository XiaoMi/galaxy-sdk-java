package com.xiaomi.infra.galaxy.sds.client.metrics;

import com.xiaomi.infra.galaxy.sds.thrift.AdminService;
import com.xiaomi.infra.galaxy.sds.thrift.ClientMetrics;
import com.xiaomi.infra.galaxy.sds.thrift.MetricData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

/**
 * Created by haxiaolin on 15-5-12.
 */
public class MetricsCollector {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsCollector.class);
  private AdminService.Iface adminService=null;
  private BlockingDeque<MetricData> queue;
  private MetricUploaderThread metricUploaderThread;

  public MetricsCollector() {
    queue = new LinkedBlockingDeque<MetricData>();
    metricUploaderThread = new MetricUploaderThread(queue);
    metricUploaderThread.setDaemon(true);
    metricUploaderThread.start();
  }

  public void setAdminService(AdminService.Iface adminService) {
    this.adminService = adminService;
  }
  public AdminService.Iface getAdminService() {
    return this.adminService;
  }

  public void collect(RequestMetrics requestMetrics) {
    queue.addAll(requestMetrics.toClientMetrics().getMetricDataList());
  }

  private class MetricUploaderThread extends Thread {

    private static final String THREAD_NAME = "sds-java-sdk-metrics-uploader";

    private final long timeoutNano = TimeUnit.MINUTES.toNanos(1);

    private final BlockingQueue<MetricData> queue;

    public MetricUploaderThread(BlockingQueue<MetricData> queue) {
      super(THREAD_NAME);
      this.queue = queue;
    }

    @Override
    public void run() {
      while (true) {
        try {
          ClientMetrics clientMetrics = nextUploadUnits();
          adminService.putClientMetrics(clientMetrics);
          LOG.info("Pushed " + clientMetrics.getMetricDataListSize() + " client metrics.");
        } catch (Exception ex) {
          LOG.warn("Unexpected exception, ignored, ", ex);
        }
      }
    }

    private ClientMetrics nextUploadUnits() throws InterruptedException {
      ClientMetrics clientMetrics = new ClientMetrics();
      long startNano = System.nanoTime();
      while (true) {
        long elapsedNano = System.nanoTime() - startNano;
        if (elapsedNano > timeoutNano) {
          return clientMetrics;
        }
        MetricData metricData = queue.poll(timeoutNano - elapsedNano,
            TimeUnit.NANOSECONDS);
        if (metricData == null) {
          // time out
          return clientMetrics;
        }
        clientMetrics.addToMetricDataList(metricData);
      }
    }
  }
}
