package com.xiaomi.infra.galaxy.ai.common;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.Map.Entry;

import javax.net.ssl.SSLContext;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLContexts;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

import com.google.common.collect.LinkedListMultimap;
import com.google.gson.Gson;
import com.xiaomi.infra.galaxy.client.authentication.HttpKeys;
import com.xiaomi.infra.galaxy.client.authentication.HttpMethod;
import com.xiaomi.infra.galaxy.client.authentication.HttpUtils;
import com.xiaomi.infra.galaxy.client.authentication.signature.SignAlgorithm;
import com.xiaomi.infra.galaxy.client.authentication.signature.Signer;
import com.xiaomi.infra.galaxy.client.authentication.signature.SubResource;

public class BaseClient {
  private static final Log LOG = LogFactory.getLog(BaseClient.class);
  private static final SignAlgorithm SIGN_ALGO = SignAlgorithm.HmacSHA1;
  protected final Credential credential;
  protected final ConnectionConfig config;
  protected final HttpClient httpClient;
  private PoolingHttpClientConnectionManager connectionManager;
  private final String clientId = UUID.randomUUID().toString().substring(0, 8);
  private final Random random = new Random();

  public BaseClient(Credential credential, ConnectionConfig fdsConfig) {
    this.credential = credential;
    this.config = fdsConfig;
    this.httpClient = createHttpClient(this.config);
  }
  
  public String getUniqueRequestId() {
    return clientId + "_" + random.nextInt();
  }

  private HttpClient createHttpClient(ConnectionConfig config) {
    RequestConfig requestConfig = RequestConfig.custom()
        .setConnectTimeout(config.getConnectionTimeoutMs())
        .setSocketTimeout(config.getSocketTimeoutMs())
        .build();

    RegistryBuilder<ConnectionSocketFactory> registryBuilder = RegistryBuilder.create();
    registryBuilder.register("http", new PlainConnectionSocketFactory());

    if (config.isHttpsEnabled()) {
      SSLContext sslContext = SSLContexts.createSystemDefault();
      SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(
          sslContext,
          SSLConnectionSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);
      registryBuilder.register("https", sslsf);
    }
    connectionManager = new PoolingHttpClientConnectionManager(registryBuilder.build());
    connectionManager.setDefaultMaxPerRoute(config.getMaxConnection());
    connectionManager.setMaxTotal(config.getMaxConnection());

    HttpClient httpClient = HttpClients.custom()
        .setConnectionManager(connectionManager)
        .setDefaultRequestConfig(requestConfig)
        .setRetryHandler(new DefaultHttpRequestRetryHandler(3, false))
        .build();
    return httpClient;
  }
  
  protected Map<String, Object> prepareRequestHeader(URI uri, HttpMethod method,
      ContentType contentType) throws IOException {
    return prepareRequestHeader(uri, method, contentType, this.credential, getUniqueRequestId());
  }
    
  public static Map<String, Object> prepareRequestHeader(URI uri, HttpMethod method,
      ContentType contentType, Credential credential, String requestId) throws IOException {
    LinkedListMultimap<String, String> headers = LinkedListMultimap.create();
    // Format date
    String date = HttpUtils.getGMTDatetime(new Date());
    headers.put(HttpKeys.DATE, date);

    // Set content type
    if (contentType != null) headers.put(HttpKeys.CONTENT_TYPE, contentType.toString());

    // Set unique request id
    headers.put(HttpKeys.REQUEST_ID, requestId);

    // Set authorization information
    String signature;
    try {
      URI relativeUri = new URI(uri.toString().substring(
        uri.toString().indexOf('/', uri.toString().indexOf(':') + 3)));
      signature = Signer.signToBase64(method, relativeUri, headers,
        credential.getAccessSecret(), SignAlgorithm.HmacSHA1);
    } catch (InvalidKeyException e) {
      LOG.error("Invalid secret key spec", e);
      throw new IOException("Invalid secret key sepc", e);
    } catch (NoSuchAlgorithmException e) {
      LOG.error("Unsupported signature algorithm:" + SIGN_ALGO, e);
      throw new IOException("Unsupported signature slgorithm:" + SIGN_ALGO, e);
    } catch (Exception e) {
      throw new IOException(e);
    }
    String authString = "Galaxy-V2 " + credential.getAccessKey() + ":" + signature;
    headers.put(HttpKeys.AUTHORIZATION, authString);

    Map<String, Object> httpHeaders = new HashMap<String, Object>();
    for (Entry<String, String> entry : headers.entries()) {
      httpHeaders.put(entry.getKey(), entry.getValue());
    }
    return httpHeaders;
  }
  
  public static Map<String, List<Object>> mergeToHeaders(Map<String, List<Object>> headers,
      Map<String, Object> header) throws IOException {
    if (headers == null) {
      headers = new HashMap<String, List<Object>>();
    }
    for (Entry<String, Object> hIte : header.entrySet()) {
      String key = hIte.getKey();
      if (!headers.containsKey(key)) {
        headers.put(key, new ArrayList<Object>());
      }
      headers.get(key).add(hIte.getValue());
    }
    return headers;
  }
  
  public static HttpUriRequest createHttpRequest(URI uri, HttpMethod method,
      Map<String, List<Object>> headers, HttpEntity requestEntity) throws IOException {
    HttpUriRequest httpRequest;
    switch (method) {
      case PUT:
        HttpPut httpPut = new HttpPut(uri);
        if (requestEntity != null)
          httpPut.setEntity(requestEntity);
        httpRequest = httpPut;
        break;
      case GET:
        httpRequest = new HttpGet(uri);
        break;
      case DELETE:
        httpRequest = new HttpDelete(uri);
        break;
      case HEAD:
        httpRequest = new HttpHead(uri);
        break;
      case POST:
        HttpPost httpPost = new HttpPost(uri);
        if (requestEntity != null)
          httpPost.setEntity(requestEntity);
        httpRequest = httpPost;
        break;
      default:
        throw new IOException("Method " + method.name() +
            " not supported");
    }
    if (headers != null) {
      for (Entry<String, List<Object>> header : headers.entrySet()) {
        String key = header.getKey();
        if (key == null || key.isEmpty()) continue;

        for (Object obj : header.getValue()) {
          if (obj == null) continue;
          httpRequest.addHeader(header.getKey(), obj.toString());
        }
      }
    }
    return httpRequest;
  }
  
  protected HttpUriRequest prepareRequestMethod(URI uri, HttpMethod method, ContentType contentType,
      HashMap<String, String> params, Map<String, List<Object>> headers, HttpEntity requestEntity)
      throws IOException {
    if (params != null) {
      URIBuilder builder = new URIBuilder(uri);
      for (Entry<String, String> param : params.entrySet()) {
        builder.addParameter(param.getKey(), param.getValue());
      }
      try {
        uri = builder.build();
      } catch (URISyntaxException e) {
        throw new IOException("Invalid param: " + params.toString(), e);
      }
    }

    Map<String, Object> h = prepareRequestHeader(uri, method, contentType);
    headers = mergeToHeaders(headers, h);
    return createHttpRequest(uri, method, headers, requestEntity);
  }
  
  protected HttpUriRequest makeJsonEntityRequest(Object requestEntity, URI uri,
      HttpMethod method) throws IOException {
    Gson gson = new Gson();
    String jsonStr = "";
    if (requestEntity != null) {
      jsonStr = gson.toJson(requestEntity);
    }
    StringEntity entity = new StringEntity(jsonStr, ContentType.APPLICATION_JSON);
    return prepareRequestMethod(uri, method, ContentType.APPLICATION_JSON, null, null, entity);
  }
  
  protected URI formatUri(String baseUri, String resource, SubResource... subResourceParams)
      throws IOException {
    String subResource = null;
    if (subResourceParams != null) {
      for (SubResource param : subResourceParams) {
        if (subResource != null) {
          subResource += "&" + param.getName();
        } else {
          subResource = param.getName();
        }
      }
    }

    try {
      URI uri = new URI(baseUri);
      String schema = uri.getScheme();
      String host = uri.getHost();
      int port = uri.getPort();
      URI encodedUri;
      if (subResource == null) {
        encodedUri = new URI(schema, null, host, port, "/" + resource,
            null, null);
      } else {
        encodedUri = new URI(schema, null, host, port, "/" + resource,
            subResource, null);
      }
      return encodedUri;
    } catch (URISyntaxException e) {
      LOG.error("Invalid uri syntax", e);
      throw new IOException("Invalid uri syntax", e);
    }
  }
  
  protected HttpResponse executeHttpRequest(HttpUriRequest httpRequest) throws IOException {
    HttpResponse response = null;
    try {
      response = httpClient.execute(httpRequest);
    } catch (IOException e) {
      LOG.error("http request failed", e);
      throw new IOException(e.getMessage(), e);
    }
    return response;
  }
  
  protected static String formatErrorMsg(String purpose, HttpResponse response) {
    String msg = "failed to " + purpose + ", status=" +
        response.getStatusLine().getStatusCode() +
        ", reason=" + getResponseEntityPhrase(response);
    return msg;
  }
  
  protected static String getResponseEntityPhrase(HttpResponse response) {
    try {
      InputStream inputStream = response.getEntity().getContent();
      ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
      byte[] data = new byte[1024];
      for (int count; (count = inputStream.read(data, 0, 1024)) != -1; )
        outputStream.write(data, 0, count);
      String reason = outputStream.toString();
      if (reason == null || reason.isEmpty())
        return response.getStatusLine().getReasonPhrase();
      return reason;
    } catch (Exception e) {
      LOG.error("Fail to get entity string");
      return response.getStatusLine().getReasonPhrase();
    }
  }
  
  protected static void closeResponseEntity(HttpResponse response) {
    if (response == null)
      return;
    HttpEntity entity = response.getEntity();
    if (entity != null && entity.isStreaming())
      try {
        entity.getContent().close();
      } catch (IOException e) {
        LOG.error("close response entity", e);
      }
  }
  
  public <T> Object processResponse(HttpResponse response, Class<T> c, String purposeStr)
      throws IOException {
    HttpEntity httpEntity = response.getEntity();
    int statusCode = response.getStatusLine().getStatusCode();
    try {
      if (statusCode == HttpStatus.SC_OK) {
        if (c != null) {
          Reader reader = new InputStreamReader(httpEntity.getContent());
          T entityVal = new Gson().fromJson(reader, c);
          return entityVal;
        }
        return null;
      } else {
        String errorMsg = formatErrorMsg(purposeStr, response);
        LOG.error(errorMsg);
        throw new IOException(errorMsg);
      }
    } catch (IOException e) {
      LOG.error("read response entity", e);
      throw new IOException("read response entity", e);
    } finally {
      closeResponseEntity(response);
    }
  }
}
