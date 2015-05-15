package com.xiaomi.infra.galaxy.sds.client;

import com.xiaomi.infra.galaxy.sds.client.metrics.MetricsCollector;
import com.xiaomi.infra.galaxy.sds.shared.clock.AdjustableClock;
import com.xiaomi.infra.galaxy.sds.thrift.AdminService;
import com.xiaomi.infra.galaxy.sds.thrift.AuthService;
import com.xiaomi.infra.galaxy.sds.thrift.CommonConstants;
import com.xiaomi.infra.galaxy.sds.thrift.Credential;
import com.xiaomi.infra.galaxy.sds.thrift.ErrorsConstants;
import com.xiaomi.infra.galaxy.sds.thrift.TableService;
import com.xiaomi.infra.galaxy.sds.thrift.ThriftProtocol;
import com.xiaomi.infra.galaxy.sds.thrift.Version;
import com.xiaomi.infra.galaxy.sds.thrift.VersionUtil;
import libthrift091.protocol.TProtocol;
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

import java.util.HashMap;
import java.util.Map;

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
  private ThriftProtocol protocol;
  private MetricsCollector metricsCollector;
  private boolean isMetricsEnabled;

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

  public ClientFactory() {
    this.credential = null;
    this.customHeaders = null;
    this.clock = new AdjustableClock();
    this.httpClient = generateHttpClient();
    this.protocol = ThriftProtocol.TBINARY;
    this.metricsCollector = null;
    this.isMetricsEnabled = false;
  }

  public ClientFactory setCredential(Credential credential) {
    this.credential = credential;
    return this;
  }

  public Credential getCredential() {
    return credential;
  }

  public ClientFactory setClock(AdjustableClock clock) {
    this.clock = clock;
    return this;
  }

  public AdjustableClock getClock() {
    return clock;
  }

  public ClientFactory setMetricsEnabled(boolean isMetricsEnabled) {
    this.isMetricsEnabled = isMetricsEnabled;
    return this;
  }

  public boolean getMetricsEnabled() {
    return isMetricsEnabled;
  }

  public ClientFactory setHttpClient(HttpClient httpClient) {
    this.httpClient = httpClient;
    return this;
  }

  public HttpClient getHttpClient() {
    return httpClient;
  }

  public ClientFactory setProtocol(ThriftProtocol protocol) {
    this.protocol = protocol;
    return this;
  }

  public ThriftProtocol getProtocol() {
    return protocol;
  }

  public ClientFactory setCustomHeaders(Map<String, String> customHeaders) {
    this.customHeaders = customHeaders;
    return this;
  }

  public Map<String, String> getCustomHeaders() {
    return customHeaders;
  }

  public Version getSDKVersion() {
    return VERSION;
  }

  public AuthService.Iface newAuthClient() {
    String url = CommonConstants.DEFAULT_SERVICE_ENDPOINT + CommonConstants.AUTH_SERVICE_PATH;
    return newAuthClient(url);
  }

  public AuthService.Iface newAuthClient(boolean supportAccountKey) {
    String url = CommonConstants.DEFAULT_SERVICE_ENDPOINT + CommonConstants.AUTH_SERVICE_PATH;
    return newAuthClient(url, supportAccountKey);
  }

  public AuthService.Iface newAuthClient(String url) {
    return newAuthClient(url, false);
  }

  public AuthService.Iface newAuthClient(String url, boolean supportAccountKey) {
    return newAuthClient(url, (int) CommonConstants.DEFAULT_CLIENT_TIMEOUT,
        (int) CommonConstants.DEFAULT_CLIENT_CONN_TIMEOUT, supportAccountKey);
  }

  public AuthService.Iface newAuthClient(String url, int socketTimeout, int connTimeout) {
    return newAuthClient(url, socketTimeout, connTimeout, false);
  }

  public AuthService.Iface newAuthClient(String url, int socketTimeout, int connTimeout,
                                         boolean supportAccountKey) {
    return createClient(AuthService.Iface.class, AuthService.Client.class, url, socketTimeout,
        connTimeout, false, ErrorsConstants.MAX_RETRY, supportAccountKey);
  }

  public AuthService.Iface newAuthClient(String url, boolean isRetry, int maxRetry) {
    return newAuthClient(url, isRetry, maxRetry, false);
  }

  public AuthService.Iface newAuthClient(String url, boolean isRetry, int maxRetry,
                                         boolean supportAccountKey) {
    return createClient(AuthService.Iface.class, AuthService.Client.class, url,
        (int) CommonConstants.DEFAULT_CLIENT_TIMEOUT,
        (int) CommonConstants.DEFAULT_CLIENT_CONN_TIMEOUT, isRetry, maxRetry, supportAccountKey);
  }

  public AuthService.Iface newAuthClient(String url, int socketTimeout, int connTimeout,
                                         boolean isRetry, int maxRetry) {
    return newAuthClient(url, socketTimeout, connTimeout, isRetry, maxRetry, false);
  }

  public AuthService.Iface newAuthClient(String url, int socketTimeout, int connTimeout,
                                         boolean isRetry, int maxRetry, boolean supportAccountKey) {
    return createClient(AuthService.Iface.class, AuthService.Client.class, url, socketTimeout,
        connTimeout, isRetry, maxRetry, supportAccountKey);
  }

  public AdminService.Iface newAdminClient() {
    if (credential == null) {
      throw new IllegalArgumentException("Credential is not set");
    }
    String url = CommonConstants.DEFAULT_SERVICE_ENDPOINT + CommonConstants.ADMIN_SERVICE_PATH;
    return newAdminClient(url);
  }

  public AdminService.Iface newAdminClient(boolean supportAccountKey) {
    if (credential == null) {
      throw new IllegalArgumentException("Credential is not set");
    }
    String url = CommonConstants.DEFAULT_SERVICE_ENDPOINT + CommonConstants.ADMIN_SERVICE_PATH;
    return newAdminClient(url, supportAccountKey);
  }

  public AdminService.Iface newAdminClient(String url) {
    return newAdminClient(url, false);
  }

  public AdminService.Iface newAdminClient(String url, boolean supportAccountKey) {
    return newAdminClient(url, (int) CommonConstants.DEFAULT_ADMIN_CLIENT_TIMEOUT,
        (int) CommonConstants.DEFAULT_CLIENT_CONN_TIMEOUT, supportAccountKey);
  }

  public AdminService.Iface newAdminClient(String url, int socketTimeout, int connTimeout) {
    return newAdminClient(url, socketTimeout, connTimeout, false);
  }

  public AdminService.Iface newAdminClient(String url, int socketTimeout, int connTimeout,
                                           boolean supportAccountKey) {
    return createClient(AdminService.Iface.class, AdminService.Client.class, url, socketTimeout,
        connTimeout, false, ErrorsConstants.MAX_RETRY, supportAccountKey);
  }

  public AdminService.Iface newAdminClient(String url, boolean isRetry, int maxRetry) {
    return newAdminClient(url, isRetry, maxRetry, false);
  }

  public AdminService.Iface newAdminClient(String url, boolean isRetry, int maxRetry,
                                           boolean supportAccountKey) {
    return createClient(AdminService.Iface.class, AdminService.Client.class, url,
        (int) CommonConstants.DEFAULT_ADMIN_CLIENT_TIMEOUT,
        (int) CommonConstants.DEFAULT_CLIENT_CONN_TIMEOUT, isRetry, maxRetry, supportAccountKey);
  }

  public AdminService.Iface newAdminClient(String url, int socketTimeout, int connTimeout,
                                           boolean isRetry, int maxRetry) {
    return newAdminClient(url, socketTimeout, connTimeout, isRetry, maxRetry, false);
  }

  public AdminService.Iface newAdminClient(String url, int socketTimeout, int connTimeout,
                                           boolean isRetry, int maxRetry, boolean supportAccountKey) {
    return createClient(AdminService.Iface.class, AdminService.Client.class, url, socketTimeout,
        connTimeout, isRetry, maxRetry, supportAccountKey);
  }

  public TableService.Iface newTableClient() {
    if (credential == null) {
      throw new IllegalArgumentException("Credential is not set");
    }
    String url = CommonConstants.DEFAULT_SERVICE_ENDPOINT + CommonConstants.TABLE_SERVICE_PATH;
    return newTableClient(url);
  }

  public TableService.Iface newTableClient(boolean supportAccountKey) {
    if (credential == null) {
      throw new IllegalArgumentException("Credential is not set");
    }
    String url = CommonConstants.DEFAULT_SERVICE_ENDPOINT + CommonConstants.TABLE_SERVICE_PATH;
    return newTableClient(url, supportAccountKey);
  }

  public TableService.Iface newTableClient(String url) {
    return newTableClient(url, false);
  }

  public TableService.Iface newTableClient(String url, boolean supportAccountKey) {
    return newTableClient(url, (int) CommonConstants.DEFAULT_CLIENT_TIMEOUT,
        (int) CommonConstants.DEFAULT_CLIENT_CONN_TIMEOUT, supportAccountKey);
  }

  public TableService.Iface newTableClient(String url, int socketTimeout, int connTimeout) {
    return newTableClient(url, socketTimeout, connTimeout, false);
  }

  public TableService.Iface newTableClient(String url, int socketTimeout, int connTimeout,
                                           boolean supportAccountKey) {
    return createClient(TableService.Iface.class, TableService.Client.class, url, socketTimeout,
        connTimeout, false, ErrorsConstants.MAX_RETRY, supportAccountKey);
  }

  public TableService.Iface newTableClient(String url, boolean isRetry, int maxRetry) {
    return newTableClient(url, isRetry, maxRetry, false);
  }

  public TableService.Iface newTableClient(String url, boolean isRetry, int maxRetry,
                                           boolean supportAccountKey) {
    return createClient(TableService.Iface.class, TableService.Client.class, url,
        (int) CommonConstants.DEFAULT_CLIENT_TIMEOUT,
        (int) CommonConstants.DEFAULT_CLIENT_CONN_TIMEOUT, isRetry, maxRetry, supportAccountKey);
  }

  public TableService.Iface newTableClient(String url, int socketTimeout, int connTimeout,
                                           boolean isRetry, int maxRetry) {
    return newTableClient(url, socketTimeout, connTimeout, isRetry, maxRetry, false);
  }

  public TableService.Iface newTableClient(String url, int socketTimeout, int connTimeout,
                                           boolean isRetry, int maxRetry, boolean supportAccountKey) {
    return createClient(TableService.Iface.class, TableService.Client.class, url, socketTimeout,
        connTimeout, isRetry, maxRetry, supportAccountKey);
  }

  private <IFace, Impl> IFace createClient(Class<IFace> ifaceClass, Class<Impl> implClass,
                                           String url, int socketTimeout, int connTimeout, boolean isRetry, int maxRetry,
                                           boolean supportAccountKey) {
    Map<String, String> headers = new HashMap<String, String>();
    headers.put(USER_AGENT_HEADER, createUserAgentHeader());
    headers.put(CommonConstants.HK_REQUEST_TIMEOUT, String.valueOf(socketTimeout));
    if (customHeaders != null) {
      headers.putAll(customHeaders);
    }
    if (isMetricsEnabled) {
      if(metricsCollector == null){
        AdminService.Iface adminClient = ThreadSafeClient.getClient(httpClient, headers, credential, clock, protocol,
            AdminService.Iface.class, AdminService.Client.class, url, (int) CommonConstants.DEFAULT_CLIENT_TIMEOUT,
            (int) CommonConstants.DEFAULT_CLIENT_CONN_TIMEOUT, false);
        AdminService.Iface metricAdminServiceClient =
            AutoRetryClient.getAutoRetryClient(AdminService.Iface.class, adminClient, true, 3);
        metricsCollector = new MetricsCollector();
        metricsCollector.setAdminService(metricAdminServiceClient);
      }
    }
    IFace client = ThreadSafeClient.getClient(httpClient, headers, credential, clock, protocol,
        ifaceClass, implClass, url, socketTimeout, connTimeout, supportAccountKey, metricsCollector);
    return AutoRetryClient.getAutoRetryClient(ifaceClass, client, isRetry, maxRetry);
  }

  protected String createUserAgentHeader() {
    return String.format("Java-SDK/%s Java/%s", VersionUtil.versionString(VERSION),
        System.getProperty("java.version"));
  }
}
