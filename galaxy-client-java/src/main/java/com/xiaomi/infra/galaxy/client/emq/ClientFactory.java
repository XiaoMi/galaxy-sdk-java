package com.xiaomi.infra.galaxy.client.emq;

import java.util.HashMap;
import java.util.Map;

import org.apache.http.client.HttpClient;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.params.ConnManagerParams;
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
import com.xiaomi.infra.galaxy.rpc.client.AutoRetryClient;
import com.xiaomi.infra.galaxy.rpc.client.ThreadSafeClient;
import com.xiaomi.infra.galaxy.rpc.util.clock.AdjustableClock;
import com.xiaomi.infra.galaxy.rpc.thrift.CommonConstants;
import com.xiaomi.infra.galaxy.rpc.thrift.Credential;
import com.xiaomi.infra.galaxy.rpc.thrift.ErrorsConstants;
import com.xiaomi.infra.galaxy.rpc.thrift.Version;
import com.xiaomi.infra.galaxy.rpc.util.VersionUtil;

/**
 * Factory to create Auth, Admin or Table clients.
 *
 * @author heliangliang
 */
public class ClientFactory {
  // Also for Android
  @SuppressWarnings("unused")
  private static final Logger LOG = LoggerFactory.getLogger(ClientFactory.class);
  private static final String USER_AGENT_HEADER = "User-Agent";
  private static final Version VERSION = new Version();

  private Credential credential;
  private Map<String, String> customHeaders;
  private HttpClient httpClient;
  private AdjustableClock clock;

  private static HttpClient generateHttpClient() {
    HttpParams params = new BasicHttpParams();
    ConnManagerParams.setMaxTotalConnections(params, 1);
    HttpConnectionParams
        .setConnectionTimeout(params, (int) CommonConstants.DEFAULT_CLIENT_CONN_TIMEOUT);
    SchemeRegistry schemeRegistry = new SchemeRegistry();
    schemeRegistry.register(
        new Scheme("http", PlainSocketFactory.getSocketFactory(), 80));
    schemeRegistry.register(new Scheme("https", SSLSocketFactory.getSocketFactory(), 443));
    ClientConnectionManager conMgr = new ThreadSafeClientConnManager(params, schemeRegistry);
    return new DefaultHttpClient(conMgr, params);
  }

  public ClientFactory setCustomHeaders(Map<String, String> customHeaders) {
    this.customHeaders = customHeaders;
    return this;
  }

  public ClientFactory() {
    this(new AdjustableClock());
  }

  public ClientFactory(Credential credential) {
    this(credential, new AdjustableClock());
  }

  public ClientFactory(AdjustableClock clock) {
    this(null, clock);
  }

  public ClientFactory(Credential credential, AdjustableClock clock) {
    this(credential, clock, generateHttpClient());
  }

  /**
   * Create client factory
   *
   * @param credential sds credential
   * @param httpClient http client, must be thread safe when used in multiple threads,
   *                   if the default client is no satisfied, the client can be set here.
   */
  public ClientFactory(Credential credential, HttpClient httpClient) {
    this(credential, new AdjustableClock(), httpClient);
  }

  /**
   * Create client factory
   *
   * @param credential sds credential
   * @param clock      adjustable clock, used for test
   * @param httpClient http client, must be thread safe when used in multiple threads,
   *                   if the default client is no satisfied, the client can be set here.
   */
  public ClientFactory(Credential credential, AdjustableClock clock, HttpClient httpClient) {
    this.credential = credential;
    this.clock = clock;
    this.httpClient = httpClient;
  }

  public QueueService.Iface newQueueClient() {
    if (credential == null) {
      throw new IllegalArgumentException("Credential is not set");
    }
    String url = "127.0.0.1:21001" + "/v1/app/queue";
    return newQueueClient(url);
  }

  public QueueService.Iface newQueueClient(String url) {
    return newQueueClient(url, (int) CommonConstants.DEFAULT_ADMIN_CLIENT_TIMEOUT,
        (int) CommonConstants.DEFAULT_CLIENT_CONN_TIMEOUT);
  }

  public QueueService.Iface newQueueClient(String url, int socketTimeout, int connTimeout) {
    return createClient(QueueService.Iface.class, QueueService.Client.class, url, socketTimeout,
        connTimeout, false, ErrorsConstants.MAX_RETRY);
  }

  public QueueService.Iface newQueueClient(String url, boolean isRetry, int maxRetry) {
    return createClient(QueueService.Iface.class, QueueService.Client.class, url,
        (int) CommonConstants.DEFAULT_ADMIN_CLIENT_TIMEOUT,
        (int) CommonConstants.DEFAULT_CLIENT_CONN_TIMEOUT, isRetry, maxRetry);
  }

  public QueueService.Iface newQueueClient(String url, int socketTimeout, int connTimeout,
      boolean isRetry, int maxRetry) {
    return createClient(QueueService.Iface.class, QueueService.Client.class, url, socketTimeout,
        connTimeout, isRetry, maxRetry);
  }

  public MessageService.Iface newMessageClient() {
    if (credential == null) {
      throw new IllegalArgumentException("Credential is not set");
    }
    String url = "127.0.0.1:21001" + "/v1/app/message";
    return newMessageClient(url);
  }

  public MessageService.Iface newMessageClient(String url) {
    return newMessageClient(url, (int) CommonConstants.DEFAULT_CLIENT_TIMEOUT,
        (int) CommonConstants.DEFAULT_CLIENT_CONN_TIMEOUT);
  }

  public MessageService.Iface newMessageClient(String url, int socketTimeout, int connTimeout) {
    return createClient(MessageService.Iface.class, MessageService.Client.class, url, socketTimeout,
        connTimeout, false, ErrorsConstants.MAX_RETRY);
  }

  public MessageService.Iface newMessageClient(String url, boolean isRetry, int maxRetry) {
    return createClient(MessageService.Iface.class, MessageService.Client.class, url,
        (int) CommonConstants.DEFAULT_CLIENT_TIMEOUT,
        (int) CommonConstants.DEFAULT_CLIENT_CONN_TIMEOUT, isRetry, maxRetry);
  }

  public MessageService.Iface newMessageClient(String url, int socketTimeout, int connTimeout,
      boolean isRetry, int maxRetry) {
    return createClient(MessageService.Iface.class, MessageService.Client.class, url, socketTimeout,
        connTimeout, isRetry, maxRetry);
  }

  private <IFace, Impl> IFace createClient(Class<IFace> ifaceClass, Class<Impl> implClass,
      String url, int socketTimeout, int connTimeout, boolean isRetry, int maxRetry) {
    Map<String, String> headers = new HashMap<String, String>();
    headers.put(USER_AGENT_HEADER, createUserAgentHeader());
    if (customHeaders != null) {
      headers.putAll(customHeaders);
    }

    IFace client = ThreadSafeClient.getClient(httpClient, headers, credential, clock,
        ifaceClass, implClass, url, socketTimeout, connTimeout, false);
    return AutoRetryClient.getAutoRetryClient(ifaceClass, client, isRetry, maxRetry);
  }

  protected String createUserAgentHeader() {
    return String.format("Java-SDK/%s Java/%s", VersionUtil.versionString(VERSION),
        System.getProperty("java.version"));
  }
}

