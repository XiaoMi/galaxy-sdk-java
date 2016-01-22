/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: yongxing@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.client;

import java.util.HashMap;
import java.util.Map;

import org.apache.http.client.HttpClient;
import org.apache.http.conn.scheme.PlainSocketFactory;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.infra.galaxy.rpc.client.AutoRetryClient;
import com.xiaomi.infra.galaxy.rpc.client.ThreadSafeClient;
import com.xiaomi.infra.galaxy.rpc.thrift.Credential;
import com.xiaomi.infra.galaxy.rpc.util.clock.AdjustableClock;
import com.xiaomi.infra.galaxy.talos.thrift.ConsumerService;
import com.xiaomi.infra.galaxy.talos.thrift.MessageService;
import com.xiaomi.infra.galaxy.talos.thrift.QuotaService;
import com.xiaomi.infra.galaxy.talos.thrift.TopicService;
import com.xiaomi.infra.galaxy.talos.thrift.Version;

public class TalosClientFactory {
  private static final Logger LOG = LoggerFactory.getLogger(TalosClientFactory.class);
  private static final String USER_AGENT_HEADER = "User-Agent";
  private static final Version VERSION = new Version();

  private TalosClientConfig talosClientConfig;
  private Credential credential;
  private Map<String, String> customHeaders;
  private HttpClient httpClient;
  private AdjustableClock clock;

  private HttpClient generateHttpClient() {
    return generateHttpClient(talosClientConfig.getMaxTotalConnections(),
        talosClientConfig.getMaxTotalConnectionsPerRoute());
  }

  public HttpClient generateHttpClient(final int maxTotalConnections,
      final int maxTotalConnectionsPerRoute) {
    return generateHttpClient(maxTotalConnections, maxTotalConnectionsPerRoute,
        talosClientConfig.getClientConnTimeout());
  }

  public HttpClient generateHttpClient(final int maxTotalConnections,
      final int maxTotalConnectionsPerRoute, int connTimeout) {
    SchemeRegistry schemeRegistry = new SchemeRegistry();
    schemeRegistry.register(new Scheme("http", 80, PlainSocketFactory.getSocketFactory()));
    schemeRegistry.register(new Scheme("https", 443, SSLSocketFactory.getSocketFactory()));

    PoolingClientConnectionManager conMgr = new PoolingClientConnectionManager(schemeRegistry);
    conMgr.setMaxTotal(maxTotalConnections);
    conMgr.setDefaultMaxPerRoute(maxTotalConnectionsPerRoute);

    HttpParams httpParams = new BasicHttpParams();
    HttpConnectionParams.setConnectionTimeout(httpParams, connTimeout);
    return new DefaultHttpClient(conMgr, httpParams);
  }

  public TalosClientFactory(TalosClientConfig talosClientConfig) {
    this(talosClientConfig, new Credential());
  }

  public TalosClientFactory(TalosClientConfig talosClientConfig,
      Credential credential) {
    this.talosClientConfig = talosClientConfig;
    this.credential = credential;
    this.customHeaders = null;
    this.httpClient = generateHttpClient();
    this.clock = new AdjustableClock();
  }

  public Credential getCredential() {
    return credential;
  }

  public TalosClientFactory setCredential(Credential credential) {
    this.credential = credential;
    return this;
  }

  public Map<String, String> getCustomHeaders() {
    return customHeaders;
  }

  public TalosClientFactory setCustomHeaders(Map<String, String> customHeaders) {
    this.customHeaders = customHeaders;
    return this;
  }

  public HttpClient getHttpClient() {
    return httpClient;
  }

  public TalosClientFactory setHttpClient(HttpClient httpClient) {
    this.httpClient = httpClient;
    return this;
  }

  public AdjustableClock getClock() {
    return clock;
  }

  public TalosClientFactory setClock(AdjustableClock clock) {
    this.clock = clock;
    return this;
  }

  public static Version getVersion() {
    return VERSION;
  }

  public TopicService.Iface newTopicClient() {
    checkCredential();
    return newTopicClient(talosClientConfig.getSecureServiceEndpoint());
  }

  public TopicService.Iface newTopicClient(String endpoint) {
    return newTopicClient(endpoint, talosClientConfig.getClientTimeout(),
        talosClientConfig.getClientConnTimeout());
  }

  public TopicService.Iface newTopicClient(String endpoint, int socketTimeout,
      int connTimeout) {
    return createClient(TopicService.Iface.class, TopicService.Client.class,
        endpoint + Constants.TALOS_TOPIC_SERVICE_PATH, socketTimeout, connTimeout,
        talosClientConfig.isRetry(), talosClientConfig.getMaxRetry());
  }

  public TopicService.Iface newTopicClient(String endpoint, boolean isRetry,
      int maxRetry) {
    return createClient(TopicService.Iface.class, TopicService.Client.class,
        endpoint + Constants.TALOS_TOPIC_SERVICE_PATH,
        talosClientConfig.getClientTimeout(),
        talosClientConfig.getClientConnTimeout(), isRetry, maxRetry);
  }

  public TopicService.Iface newTopicClient(String endpoint, int socketTimeout,
      int connTimeout, boolean isRetry, int maxRetry) {
    return createClient(TopicService.Iface.class, TopicService.Client.class,
        endpoint + Constants.TALOS_TOPIC_SERVICE_PATH,
        socketTimeout, connTimeout, isRetry, maxRetry);
  }

  public MessageService.Iface newMessageClient() {
    checkCredential();
    return newMessageClient(talosClientConfig.getSecureServiceEndpoint());
  }

  public MessageService.Iface newMessageClient(String endpoint) {
    return newMessageClient(endpoint, talosClientConfig.getClientTimeout(),
        talosClientConfig.getClientConnTimeout());
  }

  public MessageService.Iface newMessageClient(String endpoint, int socketTimeout,
      int connTimeout) {
    return createClient(MessageService.Iface.class, MessageService.Client.class,
        endpoint + Constants.TALOS_MESSAGE_SERVICE_PATH, socketTimeout,
        connTimeout, talosClientConfig.isRetry(), talosClientConfig.getMaxRetry());
  }

  public MessageService.Iface newMessageClient(String endpoint, boolean isRetry,
      int maxRetry) {
    return createClient(MessageService.Iface.class, MessageService.Client.class,
        endpoint + Constants.TALOS_MESSAGE_SERVICE_PATH,
        talosClientConfig.getClientTimeout(),
        talosClientConfig.getClientConnTimeout(), isRetry, maxRetry);
  }

  public MessageService.Iface newMessageClient(String endpoint, int socketTimeout,
      int connTimeout, boolean isRetry, int maxRetry) {
    return createClient(MessageService.Iface.class, MessageService.Client.class,
        endpoint + Constants.TALOS_MESSAGE_SERVICE_PATH, socketTimeout,
        connTimeout, isRetry, maxRetry);
  }

  public QuotaService.Iface newQuotaClient() {
    checkCredential();
    return newQuotaClient(talosClientConfig.getSecureServiceEndpoint());
  }

  public QuotaService.Iface newQuotaClient(String endpoint) {
    return newQuotaClient(endpoint, talosClientConfig.getClientTimeout(),
        talosClientConfig.getClientConnTimeout());
  }

  public QuotaService.Iface newQuotaClient(String endpoint, int socketTimeout,
      int connTimeout) {
    return createClient(QuotaService.Iface.class, QuotaService.Client.class,
        endpoint + Constants.TALOS_QUOTA_SERVICE_PATH, socketTimeout,
        connTimeout, talosClientConfig.isRetry(), talosClientConfig.getMaxRetry());
  }

  public QuotaService.Iface newQuotaClient(String endpoint, boolean isRetry,
      int maxRetry) {
    return createClient(QuotaService.Iface.class, QuotaService.Client.class,
        endpoint + Constants.TALOS_QUOTA_SERVICE_PATH,
        talosClientConfig.getClientTimeout(),
        talosClientConfig.getClientConnTimeout(), isRetry, maxRetry);
  }

  public QuotaService.Iface newQuotaClient(String endpoint, int socketTimeout,
      int connTimeout, boolean isRetry, int maxRetry) {
    return createClient(QuotaService.Iface.class, QuotaService.Client.class,
        endpoint + Constants.TALOS_QUOTA_SERVICE_PATH, socketTimeout,
        connTimeout, isRetry, maxRetry);
  }

  public ConsumerService.Iface newConsumerClient() {
    checkCredential();
    return newConsumerClient(talosClientConfig.getSecureServiceEndpoint());
  }

  public ConsumerService.Iface newConsumerClient(String endpoint) {
    return newConsumerClient(endpoint, talosClientConfig.getClientTimeout(),
        talosClientConfig.getClientConnTimeout());
  }

  public ConsumerService.Iface newConsumerClient(String endpoint,
      int socketTimeout, int connTimeout) {
    return createClient(ConsumerService.Iface.class, ConsumerService.Client.class,
        endpoint + Constants.TALOS_CONSUMER_SERVICE_PATH, socketTimeout,
        connTimeout, talosClientConfig.isRetry(), talosClientConfig.getMaxRetry());
  }

  public ConsumerService.Iface newConsumerClient(String endpoint,
      boolean isRetry, int maxRetry) {
    return createClient(ConsumerService.Iface.class, ConsumerService.Iface.class,
        endpoint + Constants.TALOS_CONSUMER_SERVICE_PATH,
        talosClientConfig.getClientTimeout(),
        talosClientConfig.getClientConnTimeout(), isRetry, maxRetry);
  }

  public ConsumerService.Iface newConsumerClient(String endpoint,
      int socketTimeout, int connTimeout, boolean isRetry, int maxRetry) {
    return createClient(ConsumerService.Iface.class, ConsumerService.Iface.class,
        endpoint + Constants.TALOS_QUOTA_SERVICE_PATH, socketTimeout,
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
    return TalosClient.getClient(ifaceClass, client);
  }

  private String createUserAgentHeader() {
    return String.format("Java-SDK/%d.%d.%d Java/%s",
        VERSION.major, VERSION.minor, VERSION.revision,
        System.getProperty("java.version"));
  }

  private void checkCredential() {
    if (credential == null) {
      throw new IllegalArgumentException("Credential is not set");
    }
  }
}
