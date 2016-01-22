package com.xiaomi.infra.galaxy.client.authentication.signature;

import java.net.URI;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.LinkedListMultimap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.xiaomi.infra.galaxy.client.authentication.HttpKeys;
import com.xiaomi.infra.galaxy.client.authentication.HttpUtils;


public class SignatureUtil {
  private static final Log LOG = LogFactory.getLog(SignatureUtil.class);

  public static boolean isPreSignedUri(URI uri) {
    Preconditions.checkNotNull(uri);
    String query = uri.getQuery();
    if (query != null) {
      LinkedListMultimap<String, String> params =
          HttpUtils.parseUriParameters(uri);
      if (params.containsKey(HttpKeys.GALAXY_ACCESS_KEY_ID) &&
          params.containsKey(HttpKeys.EXPIRES) &&
          params.containsKey(HttpKeys.SIGNATURE)) {
        return true;
      }
    }
    return false;
  }

  public static String getAccessKeyId(URI uri) {
    Preconditions.checkNotNull(uri);
    String query = uri.getQuery();
    if (query != null) {
      LinkedListMultimap<String, String> params =
          HttpUtils.parseUriParameters(uri);
      List<String> keyIds = params.get(HttpKeys.GALAXY_ACCESS_KEY_ID);
      if (!keyIds.isEmpty()) {
        return params.get(HttpKeys.GALAXY_ACCESS_KEY_ID).get(0);
      }
    }
    return null;
  }

  public static String getAccessKeyId(String authorizationValue) {
    Preconditions.checkNotNull(authorizationValue);
    String[] tokens = authorizationValue.split(" ");
    if (tokens.length == 2) {
      tokens = tokens[1].split(":");
      if (tokens.length == 2) {
        return tokens[0].trim();
      }
    }
    return null;
  }

  public static String getSignature(URI uri) {
    Preconditions.checkNotNull(uri);
    String query = uri.getQuery();
    if (query != null) {
      LinkedListMultimap<String, String> params =
          HttpUtils.parseUriParameters(uri);
      List<String> signatures = params.get(HttpKeys.SIGNATURE);
      if (!signatures.isEmpty()) {
        return params.get(HttpKeys.SIGNATURE).get(0);
      }
    }
    return null;
  }

  public static String getSignature(String authorizationValue) {
    Preconditions.checkNotNull(authorizationValue);
    String[] tokens = authorizationValue.split(" ");
    if (tokens.length == 2) {
      tokens = tokens[1].split(":");
      if (tokens.length == 2) {
        return tokens[1].trim();
      }
    }
    return null;
  }

  public static long getDateTime(LinkedListMultimap<String, String> headers) {
    List<String> dateList = headers.get(HttpKeys.MI_DATE);
    if (dateList.isEmpty()) {
      dateList = headers.get(HttpKeys.DATE);
    }

    if (!dateList.isEmpty()) {
      String datetime = dateList.get(0);
      return HttpUtils.parseDateTimeToMilliseconds(datetime);
    }
    return 0;
  }

  public static long getExpireTime(URI uri) {
    LinkedListMultimap<String, String> params = HttpUtils.parseUriParameters(uri);
    List<String> expireList = params.get(HttpKeys.EXPIRES);
    if (!expireList.isEmpty()) {
      return Long.parseLong(expireList.get(0));
    }
    return 0;
  }

  public static boolean checkExpireTime(URI uri) {
    long expireTime = getExpireTime(uri);
    if (expireTime > 0) {
      return expireTime >= System.currentTimeMillis();
    }
    return false;
  }

  public static boolean checkDateTime(LinkedListMultimap<String, String> headers) {
    long timestamp = getDateTime(headers);

    if (timestamp > 0) {
      long currentTime = System.currentTimeMillis();
      if (timestamp + HttpKeys.REQUEST_TIME_LIMIT >= currentTime) {
        return true;
      } else {
        LOG.warn("Request time is too skewed, requestTime=" + timestamp +
            ", currentTime=" + currentTime + ", skewed=" +
            (currentTime - timestamp));
      }
    } else {
      LOG.warn("Request time is not specified");
    }
    return false;
  }
}
