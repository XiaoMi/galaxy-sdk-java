package com.xiaomi.infra.galaxy.rpc.client;

import com.xiaomi.infra.galaxy.rpc.thrift.CommonConstants;
import com.xiaomi.infra.galaxy.rpc.thrift.Credential;
import com.xiaomi.infra.galaxy.rpc.thrift.Version;
import com.xiaomi.infra.galaxy.rpc.util.clock.AdjustableClock;
import com.xiaomi.infra.galaxy.rpc.util.VersionUtil;
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

import java.util.HashMap;
import java.util.Map;

/**
 * Factory to create Auth, Admin or Table clients.
 *
 * @author heliangliang
 */
public class BaseClientFactory {
  // Also for Android
  @SuppressWarnings("unused")
  private static final Logger LOG = LoggerFactory.getLogger(BaseClientFactory.class);
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
    SSLSocketFactory sslSocketFactory = SSLSocketFactory.getSocketFactory();
    sslSocketFactory.setHostnameVerifier(SSLSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);
    schemeRegistry.register(new Scheme("https", sslSocketFactory, 443));
    ClientConnectionManager conMgr = new ThreadSafeClientConnManager(params, schemeRegistry);
    return new DefaultHttpClient(conMgr, params);
  }

  public BaseClientFactory setCustomHeaders(Map<String, String> customHeaders) {
    this.customHeaders = customHeaders;
    return this;
  }

  public BaseClientFactory() {
    this(new AdjustableClock());
  }

  public BaseClientFactory(Credential credential) {
    this(credential, new AdjustableClock());
  }

  public BaseClientFactory(AdjustableClock clock) {
    this(null, clock);
  }

  public BaseClientFactory(Credential credential, AdjustableClock clock) {
    this(credential, clock, generateHttpClient());
  }

  /**
   * Create client factory
   *
   * @param credential credential
   * @param httpClient http client, must be thread safe when used in multiple threads,
   *                   if the default client is no satisfied, the client can be set here.
   */
  public BaseClientFactory(Credential credential, HttpClient httpClient) {
    this(credential, new AdjustableClock(), httpClient);
  }

  /**
   * Create client factory
   *
   * @param credential credential
   * @param clock      adjustable clock, used for test
   * @param httpClient http client, must be thread safe when used in multiple threads,
   *                   if the default client is no satisfied, the client can be set here.
   */
  public BaseClientFactory(Credential credential, AdjustableClock clock, HttpClient httpClient) {
    this.credential = credential;
    this.clock = clock;
    this.httpClient = httpClient;
  }

  public Version getSDKVersion() {
    return VERSION;
  }

  protected <IFace, Impl> IFace createClient(Class<IFace> ifaceClass, Class<Impl> implClass,
      String url, int socketTimeout, int connTimeout, boolean isRetry, int maxRetry,
      boolean supportAccountKey) {
    Map<String, String> headers = new HashMap<String, String>();
    headers.put(USER_AGENT_HEADER, createUserAgentHeader());
    headers.put(CommonConstants.HK_REQUEST_TIMEOUT, String.valueOf(socketTimeout));
    if (customHeaders != null) {
      headers.putAll(customHeaders);
    }

    IFace client = ThreadSafeClient.getClient(httpClient, headers, credential, clock,
        ifaceClass, implClass, url, socketTimeout, connTimeout, supportAccountKey);
    return AutoRetryClient.getAutoRetryClient(ifaceClass, client, isRetry, maxRetry);
  }

  protected String createUserAgentHeader() {
    return String.format("Java-SDK/%s Java/%s", VersionUtil.versionString(VERSION),
        System.getProperty("java.version"));
  }
}
