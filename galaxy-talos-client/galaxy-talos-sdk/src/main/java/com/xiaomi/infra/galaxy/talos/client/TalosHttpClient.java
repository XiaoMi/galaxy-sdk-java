/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */

package com.xiaomi.infra.galaxy.talos.client;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import com.google.common.collect.LinkedListMultimap;
import libthrift091.transport.TTransport;
import libthrift091.transport.TTransportException;
import libthrift091.transport.TTransportFactory;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.params.CoreConnectionPNames;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.infra.galaxy.client.authentication.HttpKeys;
import com.xiaomi.infra.galaxy.client.authentication.HttpMethod;
import com.xiaomi.infra.galaxy.client.authentication.HttpUtils;
import com.xiaomi.infra.galaxy.client.authentication.signature.SignAlgorithm;
import com.xiaomi.infra.galaxy.client.authentication.signature.Signer;
import com.xiaomi.infra.galaxy.rpc.thrift.AuthenticationConstants;
import com.xiaomi.infra.galaxy.rpc.thrift.CommonConstants;
import com.xiaomi.infra.galaxy.rpc.thrift.Credential;
import com.xiaomi.infra.galaxy.rpc.thrift.HttpStatusCode;
import com.xiaomi.infra.galaxy.rpc.thrift.ThriftProtocol;
import com.xiaomi.infra.galaxy.rpc.thrift.UserType;
import com.xiaomi.infra.galaxy.rpc.util.BytesUtil;
import com.xiaomi.infra.galaxy.rpc.util.DigestUtil;
import com.xiaomi.infra.galaxy.rpc.util.clock.AdjustableClock;

/**
 * HTTP implementation of the TTransport interface. Used for working with a Thrift web services
 * implementation (using for example TServlet).
 * Code based on THttpClient
 */
public class TalosHttpClient extends TTransport {
  private static final Logger LOG = LoggerFactory.getLogger(TalosHttpClient.class);
  private static final int REQUEST_ID_LENGTH = 8;
  private URL url_ = null;
  private final ByteArrayOutputStream requestBuffer_ = new ByteArrayOutputStream();
  private InputStream inputStream_ = null;
  private int connectTimeout_ = 0;
  private int socketTimeout_ = 0;
  private Map<String, String> customHeaders_ = null;
  private final HttpHost host;
  private final HttpClient client;
  private final Credential credential;
  private AdjustableClock clock;
  private ThriftProtocol protocol_ = ThriftProtocol.TCOMPACT;
  private String queryString = null;
  private boolean supportAccountKey = false;
  private String sid;

  public static class Factory extends TTransportFactory {
    private final String url;
    private final HttpClient client;
    private final Credential credential;
    private final AdjustableClock clock;

    public Factory(String url, HttpClient client, Credential credential, AdjustableClock clock) {
      this.url = url;
      this.client = client;
      this.credential = credential;
      this.clock = clock;
    }

    @Override
    public TTransport getTransport(TTransport trans) {
      try {
        return new TalosHttpClient(url, client, credential, clock);
      } catch (TTransportException tte) {
        return null;
      }
    }
  }

  public TalosHttpClient(String url, HttpClient client, Credential credential)
      throws TTransportException {
    this(url, client, credential, new AdjustableClock());
  }

  public TalosHttpClient(String url, HttpClient client, Credential credential, AdjustableClock clock)
      throws TTransportException {
    try {
      url_ = new URL(url);
      this.client = client;
      this.host = new HttpHost(url_.getHost(), -1 == url_.getPort() ? url_.getDefaultPort()
          : url_.getPort(), url_.getProtocol());
      this.credential = credential;
      this.clock = clock;
    } catch (IOException iox) {
      throw new TTransportException(iox);
    }
  }

  public int getConnectTimeout() {
    if (null != this.client) {
      return client.getParams().getIntParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, 0);
    }
    return -1;
  }

  public int getSocketTimeout() {
    if (null != this.client) {
      return client.getParams().getIntParameter(CoreConnectionPNames.SO_TIMEOUT, 0);
    }
    return -1;
  }

  public TalosHttpClient setProtocol(ThriftProtocol protocol) {
    protocol_ = protocol;
    return this;
  }

  public ThriftProtocol getProtocol() {
    return protocol_;
  }

  public TalosHttpClient setConnectTimeout(int timeout) {
    connectTimeout_ = timeout;
    if (null != this.client) {
      // WARNING, this modifies the HttpClient params, this might have an impact elsewhere if the
      // same HttpClient is used for something else.
      client.getParams().setParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, connectTimeout_);
    }
    return this;
  }

  public TalosHttpClient setSocketTimeout(int timeout) {
    socketTimeout_ = timeout;
    if (null != this.client) {
      // WARNING, this modifies the HttpClient params, this might have an impact elsewhere if the
      // same HttpClient is used for something else.
      client.getParams().setParameter(CoreConnectionPNames.SO_TIMEOUT, socketTimeout_);
    }
    return this;
  }

  public TalosHttpClient setCustomHeaders(Map<String, String> headers) {
    customHeaders_ = headers;
    return this;
  }

  public TalosHttpClient setCustomHeader(String key, String value) {
    if (customHeaders_ == null) {
      customHeaders_ = new HashMap<String, String>();
    }
    customHeaders_.put(key, value);
    return this;
  }

  public TalosHttpClient setQueryString(String queryString) {
    this.queryString = queryString;
    return this;
  }

  public TalosHttpClient setSupportAccountKey(boolean supportAccountKey) {
    this.supportAccountKey = supportAccountKey;
    return this;
  }

  public TalosHttpClient setSid(String sid) {
    this.sid = sid;
    return this;
  }

  public void open() {
  }

  public void close() {
    if (null != inputStream_) {
      try {
        inputStream_.close();
      } catch (IOException ioe) {
        ;
      }
      inputStream_ = null;
    }
  }

  public boolean isOpen() {
    return true;
  }

  public int read(byte[] buf, int off, int len) throws TTransportException {
    if (inputStream_ == null) {
      throw new TTransportException("Response buffer is empty, no request.");
    }
    try {
      int ret = inputStream_.read(buf, off, len);
      if (ret == -1) {
        throw new TTransportException("No more data available.");
      }
      return ret;
    } catch (IOException iox) {
      throw new TTransportException(iox);
    }
  }

  public void write(byte[] buf, int off, int len) {
    requestBuffer_.write(buf, off, len);
  }

  /**
   * copy from org.apache.http.util.EntityUtils#consume. Android has it's own httpcore that doesn't
   * have a consume.
   */
  private static void consume(final HttpEntity entity) throws IOException {
    if (entity == null) {
      return;
    }
    if (entity.isStreaming()) {
      InputStream instream = entity.getContent();
      if (instream != null) {
        instream.close();
      }
    }
  }

  private void flushUsingHttpClient() throws TTransportException {
    if (null == this.client) {
      throw new RuntimeException("Null HttpClient, aborting.");
    }

    // Extract request and reset buffer
    byte[] data = requestBuffer_.toByteArray();
    requestBuffer_.reset();

    HttpPost post = null;

    InputStream is = null;

    try {
      // Set request to path + query string
      String requestId = generateRandomId(REQUEST_ID_LENGTH);
      StringBuilder sb = new StringBuilder();
      sb.append(this.url_.getFile()).append("?id=").append(requestId);
      if (queryString != null) {
        sb.append("&").append(queryString);
      }
      String uri = sb.toString();
      post = new HttpPost(uri);

      //
      // Headers are added to the HttpPost instance, not
      // to HttpClient.
      //
      post.setHeader("Content-Type", CommonConstants.THRIFT_HEADER_MAP.get(protocol_));
      post.setHeader("Accept", CommonConstants.THRIFT_HEADER_MAP.get(protocol_));
      post.setHeader("User-Agent", "Java/THttpClient/HC");
      setCustomHeaders(post);
      setAuthenticationHeaders(post, data, supportAccountKey);

      post.setEntity(new ByteArrayEntity(data));

      HttpResponse response = this.client.execute(this.host, post);
      int responseCode = response.getStatusLine().getStatusCode();
      String reasonPhrase = response.getStatusLine().getReasonPhrase();

      //
      // Retrieve the inputstream BEFORE checking the status code so
      // resources get freed in the finally clause.
      //

      is = response.getEntity().getContent();
      // Read the responses into a byte array so we can release the connection
      // early. This implies that the whole content will have to be read in
      // memory, and that momentarily we might use up twice the memory (while the
      // thrift struct is being read up the chain).
      // Proceeding differently might lead to exhaustion of connections and thus
      // to app failure.

      byte[] buf = new byte[1024];
      ByteArrayOutputStream baos = new ByteArrayOutputStream();

      int len = 0;
      do {
        len = is.read(buf);
        if (len > 0) {
          baos.write(buf, 0, len);
        }
      } while (-1 != len);

      try {
        // Indicate we're done with the content.
        consume(response.getEntity());
      } catch (IOException ioe) {
        // We ignore this exception, it might only mean the server has no
        // keep-alive capability.
      }
 
      if (responseCode != HttpStatus.SC_OK) {
        Header[] typeHeader = response.getHeaders("Content-Type");
        if (typeHeader.length == 0 ||
            !typeHeader[0].getValue().startsWith("application/x-thrift")) {
          adjustClock(response, responseCode);
          throw new HttpTTransportException(responseCode, reasonPhrase + ":" +
              new String(baos.toByteArray(), "UTF-8"));
        }
      }

     inputStream_ = new ByteArrayInputStream(baos.toByteArray());
    } catch (IOException ioe) {
      // Abort method so the connection gets released back to the connection manager
      if (null != post) {
        post.abort();
      }
      throw new TTransportException(ioe);
    } finally {
      if (null != is) {
        // Close the entity's input stream, this will release the underlying connection
        try {
          is.close();
        } catch (IOException ioe) {
          throw new TTransportException(ioe);
        }
      }
    }
  }

  public void flush() throws TTransportException {
    if (this.client == null) {
      throw new RuntimeException("not supported");
    }
    flushUsingHttpClient();
  }

  private TalosHttpClient setCustomHeaders(HttpPost post) {
    if (this.client != null && this.customHeaders_ != null) {
      for (Map.Entry<String, String> header : customHeaders_.entrySet()) {
        post.setHeader(header.getKey(), header.getValue());
      }
    }
    return this;
  }

  /**
   * Set signature related headers when credential is properly set
   */
  private TalosHttpClient setAuthenticationHeaders(HttpPost post, byte[] data, boolean supportAccountKey) {
    if (this.client != null && credential != null) {
      if (credential.getType() != null && credential.getSecretKeyId() != null) {

        // signature is supported
        if (AuthenticationConstants.SIGNATURE_SUPPORT.get(credential.getType())) {
          // host
          String host = this.host.toHostString();
          host = host.split(":")[0];
          post.setHeader(AuthenticationConstants.HK_HOST, host);

          // timestamp
          String timestamp = Long.toString(clock.getCurrentEpoch());
          post.setHeader(AuthenticationConstants.HK_TIMESTAMP, timestamp);
          post.setHeader(HttpKeys.MI_DATE, HttpUtils.getGMTDatetime(new Date()));

          // content md5
          String md5 = BytesUtil
              .bytesToHex(DigestUtil.digest(DigestUtil.DigestAlgorithm.MD5, data));
          post.setHeader(AuthenticationConstants.HK_CONTENT_MD5, md5);

          // signature
          LinkedListMultimap<String, String> headers = LinkedListMultimap.create();
          for (Header header : post.getAllHeaders()) {
            headers.put(header.getName().toLowerCase(), header.getValue());
          }

          // generate authString for header
          try {
            String authType = "Galaxy-V2 ";
            if (supportAccountKey) {
              authType = "Galaxy-V3 ";
            }
            if (credential.getType() == UserType.APP_ACCESS_TOKEN) {
              authType = "OAuth ";
            }

            // check secretKeyId and set header attached info
            // secretKeyId format: "Service-Admin#SecretKeyId#developerId"
            String secretKeyId = credential.getSecretKeyId();
            if (secretKeyId.contains(AuthenticationConstants.HK_SERVICE_ADMIN)) {
              String[] items = secretKeyId.split(AuthenticationConstants.HK_SERVICE_MARK);
              if (items.length != 3) {
                throw new RuntimeException("Invalid credential secretKeyId, " +
                    "expected: 3, actual: " + items.length);
              }

              // reset secretKeyId and add header attached info
              secretKeyId = items[1];
              post.setHeader(AuthenticationConstants.HK_ATTACHED_INFO, items[2]);
            }

            String authString = authType + secretKeyId + ":" +
                Signer.signToBase64(HttpMethod.POST, post.getURI(), headers,
                    credential.getSecretKey(), SignAlgorithm.HmacSHA1);
            post.setHeader(AuthenticationConstants.HK_AUTHORIZATION, authString);
          } catch (Exception e) {
            throw new RuntimeException("Failed to sign", e);
          }
        } else {
          if (credential.getType() == UserType.APP_XIAOMI_SSO) {
            String authString = "SSO " + sid + ":" + credential.getSecretKey() + ":" +
                credential.getSecretKeyId();
            post.setHeader(AuthenticationConstants.HK_AUTHORIZATION, authString);
          } else if (credential.getType() == UserType.APP_ANONYMOUS) {
            String authString = "Guest " + credential.getSecretKeyId();
            post.setHeader(AuthenticationConstants.HK_AUTHORIZATION, authString);
          } else {
            throw new RuntimeException("Unsupported user type: " + credential.getType());
          }
        } // else
      }
    }
    return this;
  }

  /**
   * Adjust local clock when clock skew error received from server. The client clock need to be
   * roughly synchronized with server clock to make signature secure and reduce the chance of replay
   * attacks.
   *
   * @param response       server response
   * @param httpStatusCode status code
   * @return if clock is adjusted
   */
  private boolean adjustClock(HttpResponse response, int httpStatusCode) {
    if (httpStatusCode == HttpStatusCode.CLOCK_TOO_SKEWED.getValue()) {
      Header[] headers = response.getHeaders(AuthenticationConstants.HK_TIMESTAMP);
      for (Header h : headers) {
        String hv = h.getValue();
        long serverTime = Long.parseLong(hv);
        long min = 60 * 60 * 24 * 365 * (2010 - 1970);
        long max = 60 * 60 * 24 * 365 * (2030 - 1970);
        if (serverTime > min && serverTime < max) {
          LOG.debug("Adjusting client time from {} to {}",
              new Date(clock.getCurrentEpoch() * 1000), new Date(serverTime * 1000));
          clock.adjust(serverTime);
          return true;
        }
      }
    }
    return false;
  }

  private static String generateRandomId(int length) {
    return UUID.randomUUID().toString().substring(0, length);
  }
}

