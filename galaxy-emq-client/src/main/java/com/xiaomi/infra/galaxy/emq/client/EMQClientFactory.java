package com.xiaomi.infra.galaxy.emq.client;

import java.util.HashMap;
import java.util.Map;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.infra.galaxy.emq.thrift.MessageService;
import com.xiaomi.infra.galaxy.emq.thrift.QueueService;
import com.xiaomi.infra.galaxy.emq.thrift.Version;
import com.xiaomi.infra.galaxy.rpc.client.AutoRetryClient;
import com.xiaomi.infra.galaxy.rpc.client.ThreadSafeClient;
import com.xiaomi.infra.galaxy.rpc.thrift.Credential;
import com.xiaomi.infra.galaxy.rpc.thrift.ErrorsConstants;
import com.xiaomi.infra.galaxy.rpc.util.clock.AdjustableClock;

/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: shenyuannan@xiaomi.com
 */

public class EMQClientFactory {
  // Also for Android
  @SuppressWarnings("unused")
  private static final Logger LOG = LoggerFactory.getLogger(EMQClientFactory.class);

  private static final String USER_AGENT_HEADER = "User-Agent";
  private static final Version VERSION = new Version();

  private Credential credential;
  private Map<String, String> customHeaders;
  private HttpClient httpClient;
  private AdjustableClock clock;

  private static HttpClient generateHttpClient() {
    return generateHttpClient(1, 1);
  }

  public static HttpClient generateHttpClient(final int maxTotalConnections,
      final int maxTotalConnectionsPerRoute) {
    return generateHttpClient(maxTotalConnections, maxTotalConnectionsPerRoute,
        EMQConstants.DEFAULT_CLIENT_CONN_TIMEOUT);
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
    sslSocketFactory.setHostnameVerifier(SSLSocketFactory.
        ALLOW_ALL_HOSTNAME_VERIFIER);
    schemeRegistry.register(new Scheme("https", sslSocketFactory, 443));
    ClientConnectionManager conMgr = new ThreadSafeClientConnManager(params,
        schemeRegistry);
    return new DefaultHttpClient(conMgr, params);
  }

  public Credential getCredential() {
    return credential;
  }

  public EMQClientFactory setCredential(Credential credential) {
    this.credential = credential;
    return this;
  }

  public Map<String, String> getCustomHeaders() {
    return customHeaders;
  }

  public EMQClientFactory setCustomHeaders(Map<String, String> customHeaders) {
    this.customHeaders = customHeaders;
    return this;
  }

  public HttpClient getHttpClient() {
    return httpClient;
  }

  public EMQClientFactory setHttpClient(HttpClient httpClient) {
    this.httpClient = httpClient;
    return this;
  }

  public Version getSDKVersion() {
    return VERSION;
  }

  public EMQClientFactory() {
    this(new AdjustableClock());
  }

  public EMQClientFactory(Credential credential) {
    this(credential, new AdjustableClock());
  }

  public EMQClientFactory(AdjustableClock clock) {
    this(null, clock);
  }

  public EMQClientFactory(Credential credential, AdjustableClock clock) {
    this(credential, clock, generateHttpClient());
  }

  /**
   * Create client factory
   *
   * @param credential emq credential
   * @param httpClient http client, must be thread safe when used in multiple
   *                   threads, if the default client is no satisfied,
   *                   the client can be set here.
   */
  public EMQClientFactory(Credential credential, HttpClient httpClient) {
    this(credential, new AdjustableClock(), httpClient);
  }

  /**
   * Create client factory
   *
   * @param credential emq credential
   * @param clock      adjustable clock, used for test
   * @param httpClient http client, must be thread safe when used in multiple
   *                   threads, if the default client is no satisfied,
   *                   the client can be set here.
   */
  public EMQClientFactory(Credential credential, AdjustableClock clock,
      HttpClient httpClient) {
    this.credential = credential;
    this.clock = clock;
    this.httpClient = httpClient;
  }

  public QueueService.Iface newQueueClient() {
    if (credential == null) {
      throw new IllegalArgumentException("Credential is not set");
    }
    return newQueueClient(EMQConstants.DEFAULT_SECURE_SERVICE_ENDPOINT);
  }

  public QueueService.Iface newQueueClient(String endpoint) {
    return newQueueClient(endpoint, EMQConstants.DEFAULT_CLIENT_TIMEOUT,
        EMQConstants.DEFAULT_CLIENT_CONN_TIMEOUT);
  }

  public QueueService.Iface newQueueClient(String endpoint, int socketTimeout,
      int connTimeout) {
    return createClient(QueueService.Iface.class, QueueService.Client.class,
        endpoint + EMQConstants.QUEUE_SERVICE_PATH, socketTimeout, connTimeout,
        false, ErrorsConstants.MAX_RETRY);
  }

  public QueueService.Iface newQueueClient(String endpoint, boolean isRetry,
      int maxRetry) {
    return createClient(QueueService.Iface.class, QueueService.Client.class,
        endpoint + EMQConstants.QUEUE_SERVICE_PATH,
        EMQConstants.DEFAULT_CLIENT_TIMEOUT,
        EMQConstants.DEFAULT_CLIENT_CONN_TIMEOUT, isRetry, maxRetry);
  }

  public QueueService.Iface newQueueClient(String endpoint, int socketTimeout,
      int connTimeout, boolean isRetry, int maxRetry) {
    return createClient(QueueService.Iface.class, QueueService.Client.class,
        endpoint + EMQConstants.QUEUE_SERVICE_PATH,
        socketTimeout, connTimeout, isRetry, maxRetry);
  }

  public MessageService.Iface newMessageClient() {
    if (credential == null) {
      throw new IllegalArgumentException("Credential is not set");
    }
    return newMessageClient(EMQConstants.DEFAULT_SECURE_SERVICE_ENDPOINT);
  }

  public MessageService.Iface newMessageClient(String endpoint) {
    return newMessageClient(endpoint, EMQConstants.DEFAULT_CLIENT_TIMEOUT,
        EMQConstants.DEFAULT_CLIENT_CONN_TIMEOUT);
  }

  public MessageService.Iface newMessageClient(String endpoint, int socketTimeout,
      int connTimeout) {
    return createClient(MessageService.Iface.class, MessageService.Client.class,
        endpoint + EMQConstants.MESSAGE_SERVICE_PATH, socketTimeout,
        connTimeout, false, ErrorsConstants.MAX_RETRY);
  }

  public MessageService.Iface newMessageClient(String endpoint, boolean isRetry,
      int maxRetry) {
    return createClient(MessageService.Iface.class, MessageService.Client.class,
        endpoint + EMQConstants.MESSAGE_SERVICE_PATH,
        EMQConstants.DEFAULT_CLIENT_TIMEOUT,
        EMQConstants.DEFAULT_CLIENT_CONN_TIMEOUT, isRetry, maxRetry);
  }

  public MessageService.Iface newMessageClient(String endpoint, int socketTimeout,
      int connTimeout, boolean isRetry, int maxRetry) {
    return createClient(MessageService.Iface.class, MessageService.Client.class,
        endpoint + EMQConstants.MESSAGE_SERVICE_PATH, socketTimeout,
        connTimeout, isRetry, maxRetry);
  }

  private <IFace, Impl> IFace createClient(Class<IFace> ifaceClass,
      Class<Impl> implClass, String url, int socketTimeout, int connTimeout,
      boolean isRetry, int maxRetry) {
    Map<String, String> headers = new HashMap<String, String>();
    headers.put(USER_AGENT_HEADER, createUserAgentHeader());
    if (customHeaders != null) {
      headers.putAll(customHeaders);
    }

    IFace client = ThreadSafeClient.getClient(httpClient, headers, credential,
        clock, ifaceClass, implClass, url, socketTimeout, connTimeout, false);
    client = AutoRetryClient.getAutoRetryClient(ifaceClass, client, isRetry,
        maxRetry);
    return EMQClient.getClient(ifaceClass, client);
  }

  protected String createUserAgentHeader() {
    return String.format("Java-SDK/%d.%d.%d Java/%s",
        VERSION.major, VERSION.minor, VERSION.revision,
        System.getProperty("java.version"));
  }
}

