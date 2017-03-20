package com.xiaomi.infra.galaxy.metrics.client;

import com.xiaomi.infra.galaxy.metrics.thrift.CommonConstants;
import com.xiaomi.infra.galaxy.metrics.thrift.Credential;
import com.xiaomi.infra.galaxy.metrics.thrift.DashboardService;
import com.xiaomi.infra.galaxy.metrics.thrift.ErrorsConstants;
import com.xiaomi.infra.galaxy.metrics.thrift.JudgeService;
import com.xiaomi.infra.galaxy.metrics.thrift.MetricsService;
import com.xiaomi.infra.galaxy.metrics.thrift.ThriftProtocol;
import com.xiaomi.infra.galaxy.metrics.thrift.Version;
import com.xiaomi.infra.galaxy.rpc.util.clock.AdjustableClock;
import org.apache.http.client.HttpClient;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.params.ConnManagerParams;
import org.apache.http.conn.params.ConnPerRoute;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.conn.scheme.PlainSocketFactory;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;

import java.util.HashMap;
import java.util.Map;

/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: linshangquan@xiaomi.com
 */
public class MetricsClientFactory {
  private static final String USER_AGENT_HEADER = "User-Agent";
  private static final Version VERSION = new Version();

  private Credential credential;
  private Map<String, String> customHeaders;
  private HttpClient httpClient;
  private AdjustableClock clock;
  private ThriftProtocol protocol;

  private static HttpClient generateHttpClient() {
    return generateHttpClient(1, 1, (int) CommonConstants.DEFAULT_CLIENT_CONN_TIMEOUT);
  }

  public static HttpClient generateHttpClient(final int maxTotalConnections,
      final int maxTotalConnectionsPerRoute) {
    return generateHttpClient(maxTotalConnections, maxTotalConnectionsPerRoute,
        (int) CommonConstants.DEFAULT_CLIENT_CONN_TIMEOUT);
  }

  public static HttpClient generateHttpClient(final int maxTotalConnections,
      final int maxTotalConnectionsPerRoute, int connTimeout) {
    HttpParams params = new BasicHttpParams();
    ConnManagerParams.setMaxTotalConnections(params, maxTotalConnections);
    ConnManagerParams.setMaxConnectionsPerRoute(params, new ConnPerRoute() {
      @Override
      public int getMaxForRoute(HttpRoute route) {
        return maxTotalConnectionsPerRoute;
      }
    });
    HttpConnectionParams
        .setConnectionTimeout(params, connTimeout);
    SchemeRegistry schemeRegistry = new SchemeRegistry();
    schemeRegistry.register(
        new Scheme("http", PlainSocketFactory.getSocketFactory(), 80));
    SSLSocketFactory sslSocketFactory = SSLSocketFactory.getSocketFactory();
    sslSocketFactory.setHostnameVerifier(SSLSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);
    schemeRegistry.register(new Scheme("https", sslSocketFactory, 443));
    ClientConnectionManager conMgr = new ThreadSafeClientConnManager(params, schemeRegistry);
    return new DefaultHttpClient(conMgr, params);
  }

  public MetricsClientFactory() {
    this.credential = null;
    this.customHeaders = null;
    this.clock = new AdjustableClock();
    this.httpClient = generateHttpClient();
    this.protocol = ThriftProtocol.TBINARY;
  }

  public MetricsClientFactory setCredential(Credential credential) {
    this.credential = credential;
    return this;
  }

  public Credential getCredential() {
    return credential;
  }

  public MetricsClientFactory setClock(AdjustableClock clock) {
    this.clock = clock;
    return this;
  }

  public AdjustableClock getClock() {
    return clock;
  }

  public MetricsClientFactory setHttpClient(HttpClient httpClient) {
    this.httpClient = httpClient;
    return this;
  }

  public HttpClient getHttpClient() {
    return httpClient;
  }

  public MetricsClientFactory setProtocol(ThriftProtocol protocol) {
    this.protocol = protocol;
    return this;
  }

  public ThriftProtocol getProtocol() {
    return protocol;
  }

  public MetricsClientFactory setCustomHeaders(Map<String, String> customHeaders) {
    this.customHeaders = customHeaders;
    return this;
  }

  public Map<String, String> getCustomHeaders() {
    return customHeaders;
  }

  public Version getSDKVersion() {
    return VERSION;
  }

  public MetricsService.Iface newMetricsClient(String url) {
    return newMetricsClient(url, (int) CommonConstants.DEFAULT_CLIENT_TIMEOUT,
        (int) CommonConstants.DEFAULT_CLIENT_CONN_TIMEOUT, true, ErrorsConstants.MAX_RETRY);
  }

  public MetricsService.Iface newMetricsClient(String url, int socketTimeout, int connTimeout) {
    return newMetricsClient(url, socketTimeout, connTimeout, true, ErrorsConstants.MAX_RETRY);
  }


  public DashboardService.Iface newDashboardClient(String url, int socketTimeout, int connTimeout,
      boolean isRetry, int retryTime) {
    return createClient(DashboardService.Iface.class, DashboardService.Client.class,
        url, socketTimeout, connTimeout, isRetry, retryTime);
  }

  public DashboardService.Iface newDashboardClient(String url) {
    return newDashboardClient(url, (int) CommonConstants.DEFAULT_CLIENT_TIMEOUT,
        (int) CommonConstants.DEFAULT_CLIENT_CONN_TIMEOUT, true, ErrorsConstants.MAX_RETRY);
  }

  public DashboardService.Iface newDashboardClient(String url, int socketTimeout, int connTimeout) {
    return newDashboardClient(url, socketTimeout, connTimeout, true, ErrorsConstants.MAX_RETRY);
  }


  public MetricsService.Iface newMetricsClient(String url, int socketTimeout, int connTimeout,
      boolean isRetry, int retryTime) {
    return createClient(MetricsService.Iface.class, MetricsService.Client.class,
        url, socketTimeout, connTimeout, isRetry, retryTime);
  }

  public JudgeService.Iface newJudgeClient(String url) {
    return newJudgeClient(url, (int) CommonConstants.DEFAULT_CLIENT_TIMEOUT,
        (int) CommonConstants.DEFAULT_CLIENT_CONN_TIMEOUT, true, ErrorsConstants.MAX_RETRY);
  }

  public JudgeService.Iface newJudgeClient(String url, int socketTimeout, int connTimeout) {
    return newJudgeClient(url, socketTimeout, connTimeout, true, ErrorsConstants.MAX_RETRY);
  }


  public JudgeService.Iface newJudgeClient(String url, int socketTimeout, int connTimeout,
      boolean isRetry, int retryTime) {
    return createClient(JudgeService.Iface.class, JudgeService.Client.class,
        url, socketTimeout, connTimeout, isRetry, retryTime);
  }

  private <IFace, Impl> IFace createClient(Class<IFace> ifaceClass, Class<Impl> implClass,
      String url, int socketTimeout, int connTimeout, boolean isRetry, int maxRetry) {
    Map<String, String> headers = new HashMap<String, String>();
    headers.put(USER_AGENT_HEADER, createUserAgentHeader());
    headers.put(CommonConstants.HK_REQUEST_TIMEOUT, String.valueOf(socketTimeout));
    if (customHeaders != null) {
      headers.putAll(customHeaders);
    }

    IFace client = MetricsThreadSafeClient.getClient(httpClient, headers, credential, clock, protocol,
        ifaceClass, implClass, url, socketTimeout, connTimeout);
    return MetricsAutoRetryClient.getAutoRetryClient(ifaceClass, client, isRetry, maxRetry);
  }

  private static String versionString(Version version) {
    return String.format("%d.%d.%s", version.major, version.minor, version.patch);
  }

  private String createUserAgentHeader() {
    return String.format("Java-SDK/%s Java/%s", versionString(VERSION),
        System.getProperty("java.version"));
  }
}
