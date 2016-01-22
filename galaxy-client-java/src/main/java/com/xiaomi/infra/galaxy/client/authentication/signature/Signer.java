package com.xiaomi.infra.galaxy.client.authentication.signature;

import java.net.URI;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import com.google.common.base.Preconditions;
import com.google.common.collect.LinkedListMultimap;
import com.sun.org.apache.xerces.internal.impl.dv.util.Base64;

import com.xiaomi.infra.galaxy.client.authentication.HttpKeys;
import com.xiaomi.infra.galaxy.client.authentication.HttpMethod;

public class Signer {
  private static final Set<String> SUB_RESOURCE_SET = new HashSet<String>();
  private static final String XIAOMI_DATE = HttpKeys.MI_DATE;

  static {
    for (SubResource r : SubResource.values()) {
      SUB_RESOURCE_SET.add(r.getName());
    }
  }

  /**
   * Sign the specified http request.
   *
   * @param httpMethod  The http request method
   * @param uri The uri string
   * @param httpHeaders The http request headers
   * @param secretAccessKeyId The user's secret access key
   * @param algorithm   The sign algorithm
   * @return Byte buffer of the signed result
   * @throws java.security.NoSuchAlgorithmException
   * @throws java.security.InvalidKeyException
   */
  public static byte[] sign(HttpMethod httpMethod, URI uri,
      LinkedListMultimap<String, String> httpHeaders, String secretAccessKeyId,
      SignAlgorithm algorithm) throws NoSuchAlgorithmException,
      InvalidKeyException {
    Preconditions.checkNotNull(httpMethod, "Http method");
    Preconditions.checkNotNull(uri, "uri");
    Preconditions.checkNotNull(secretAccessKeyId, "secret access key");
    Preconditions.checkNotNull(algorithm, "algorithm");

    String stringToSign = constructStringToSign(httpMethod,
        uri, httpHeaders);

    Mac mac = Mac.getInstance(algorithm.name());
    mac.init(new SecretKeySpec(secretAccessKeyId.getBytes(), algorithm.name()));
    return mac.doFinal(stringToSign.getBytes());
  }

  /**
   * A handy version of {@link #sign(HttpMethod, java.net.URI, LinkedListMultimap,
   * String, SignAlgorithm)}, generates base64 encoded sign result.
   */
  public static String signToBase64(HttpMethod httpMethod, URI uri,
      LinkedListMultimap<String, String> httpHeaders, String secretAccessKeyId,
      SignAlgorithm algorithm) throws NoSuchAlgorithmException,
      InvalidKeyException {
    return Base64.encode(sign(httpMethod, uri, httpHeaders, secretAccessKeyId,
        algorithm));
  }

  /**
   * @see #sign(HttpMethod, java.net.URI, LinkedListMultimap, String, SignAlgorithm)
   * @see #signToBase64(HttpMethod, java.net.URI, LinkedListMultimap, String, SignAlgorithm)
   *
   * @param httpMethod  The http request method
   * @param uri The uri string
   * @param httpHeaders The http request headers
   * @param accessKeyId The user's access key
   * @param secretAccessKeyId The user's secret access key
   * @param algorithm   The sign algorithm
   * @return return the value of Authorization Http header
   */
  public static String getAuthorizationHeader(HttpMethod httpMethod, URI uri,
      LinkedListMultimap<String, String> httpHeaders, String accessKeyId,
      String secretAccessKeyId, SignAlgorithm algorithm)
      throws NoSuchAlgorithmException, InvalidKeyException {
    String signature = signToBase64(httpMethod, uri, httpHeaders,
        secretAccessKeyId, algorithm);
    return "Galaxy-V2 " + accessKeyId + ":" + signature;
  }

  private static LinkedListMultimap<String, String> parseUriParameters(URI uri) {
    LinkedListMultimap<String, String> params = LinkedListMultimap.create();
    String query = uri.getQuery();
    if (query != null) {
      for (String param : query.split("&")) {
        String[] kv = param.split("=");
        if (kv.length >= 2) {
          params.put(kv[0], param.substring(kv[0].length() + 1));
        } else {
          params.put(kv[0], "");
        }
      }
    }
    return params;
  }


  static String constructStringToSign(HttpMethod httpMethod, URI uri,
      LinkedListMultimap<String, String> httpHeaders) {
    StringBuilder builder = new StringBuilder();
    builder.append(httpMethod.name()).append("\n");

    builder.append(checkAndGet(httpHeaders,
        HttpKeys.CONTENT_MD5).get(0)).append("\n");
    builder.append(checkAndGet(httpHeaders,
        HttpKeys.CONTENT_TYPE).get(0)).append("\n");

    long expires;
    if ((expires = getExpires(uri)) > 0) {
      // For pre-signed URI
      builder.append(expires).append("\n");
    } else {
      String xiaomiDate = checkAndGet(httpHeaders, XIAOMI_DATE).get(0);
      String date = "";
      if ("".equals(xiaomiDate)) {
        date = checkAndGet(httpHeaders, HttpKeys.DATE).get(0);
      }
      builder.append(checkAndGet(date)).append("\n");
    }

    builder.append(canonicalizeXiaomiHeaders(httpHeaders));
    builder.append(canonicalizeResource(uri));
    return builder.toString();
  }

  static String canonicalizeXiaomiHeaders(
      LinkedListMultimap<String, String> headers) {
    if (headers == null) {
      return "";
    }

    // 1. Sort the header and merge the values
    Map<String, String> sortedHeaders = new TreeMap<String, String>();
    for (String key : headers.keySet()) {
      if (!key.toLowerCase().startsWith(HttpKeys.XIAOMI_HEADER_PREFIX)) {
        continue;
      }

      StringBuilder builder = new StringBuilder();
      int index = 0;
      for (String value : headers.get(key)) {
        if (index != 0) {
          builder.append(",");
        }
        builder.append(value);
        index++;
      }
      sortedHeaders.put(key, builder.toString());
    }

    // 3. Generate the canonicalized result
    StringBuilder result = new StringBuilder();
    for (Entry<String, String> entry : sortedHeaders.entrySet()) {
      result.append(entry.getKey()).append(":")
          .append(entry.getValue()).append("\n");
    }
    return result.toString();
  }

  static String canonicalizeResource(URI uri) {
    StringBuilder result = new StringBuilder();
    result.append(uri.getPath());

    // 1. Parse and sort subresources
    TreeMap<String, String> sortedParams = new TreeMap<String, String>();
    LinkedListMultimap<String, String> params = parseUriParameters(uri);
    for (String key : params.keySet()) {
      for (String value : params.get(key)) {
        if (SUB_RESOURCE_SET.contains(key)) {
          sortedParams.put(key, value);
        }
      }
    }

    // 2. Generate the canonicalized result
    if (!sortedParams.isEmpty()) {
      result.append("?");
      boolean isFirst = true;
      for (Entry<String, String> entry : sortedParams.entrySet()) {
        if (isFirst) {
          isFirst = false;
          result.append(entry.getKey());
        } else {
          result.append("&").append(entry.getKey());
        }

        if (!entry.getValue().isEmpty()) {
          result.append("=").append(entry.getValue());
        }
      }
    }
    return result.toString();
  }

  static String checkAndGet(String name) {
    return name == null ? "" : name;
  }

  static List<String> checkAndGet(LinkedListMultimap<String, String> headers,
      String header) {
    List<String> result = new LinkedList<String>();
    if (headers == null) {
      result.add("");
      return result;
    }

    List<String> values = headers.get(header);
    if (values == null || values.isEmpty()) {
      result.add("");
      return result;
    }
    return values;
  }

  static long getExpires(URI uri) {
    LinkedListMultimap<String, String> params = parseUriParameters(uri);
    List<String> expires = params.get(HttpKeys.EXPIRES);
    if (expires != null && !expires.isEmpty()) {
      return Long.parseLong(expires.get(0));
    }
    return 0;
  }
}
