package com.xiaomi.infra.galaxy.client.authentication;

import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;

import com.google.common.collect.LinkedListMultimap;

public class HttpUtils {

  // According to <a href="http://www.w3.org/Protocols/rfc2616/rfc2616-sec3.html">
  // Protocol Parameters</a>, HTTP applications have historically allowed three
  // different formats for the representation of date/time stamps:
  //  Sun, 06 Nov 1994 08:49:37 GMT  ; RFC 822, updated by RFC 1123
  //  Sunday, 06-Nov-94 08:49:37 GMT ; RFC 850, obsoleted by RFC 1036
  //  Sun Nov  6 08:49:37 1994       ; ANSI C's asctime() format
  private static final ThreadLocal<SimpleDateFormat> RFC_822_DATE_FORMAT =
      new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() {
          SimpleDateFormat format = new SimpleDateFormat(
              "EEE, dd MMM yyyy HH:mm:ss z", Locale.US);
          format.setTimeZone(TimeZone.getTimeZone("GMT"));
          return format;
        }
      };

  private static final ThreadLocal<SimpleDateFormat> RFC_850_DATE_FORMAT =
      new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() {
          SimpleDateFormat format = new SimpleDateFormat(
              "EEEE, dd-MMM-yy HH:mm:ss z", Locale.US);
          format.setTimeZone(TimeZone.getTimeZone("GMT"));
          return format;
        }
      };

  private static final ThreadLocal<SimpleDateFormat> ANSI_DATE_FORMAT =
      new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() {
          SimpleDateFormat format = new SimpleDateFormat(
              "EEE MMM d HH:mm:ss yyyy", Locale.US);
          format.setTimeZone(TimeZone.getTimeZone("GMT"));
          return format;
        }
      };

  public static LinkedListMultimap<String, String> parseUriParameters(URI uri) {
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

  public static String getSingleHeader(String header,
      LinkedListMultimap<String, String> headers) {
    List<String> values = headers.get(header);
    if (values == null) {
      values = headers.get(header.toLowerCase());
    }

    if (values != null && !values.isEmpty()) {
      return values.get(0);
    }
    return null;
  }

  public static String getSingleParam(String name, URI uri) {
    LinkedListMultimap<String, String> params = parseUriParameters(uri);
    if (params != null && !params.isEmpty()) {
      List<String> values = params.get(name);
      if (values == null) {
        values = params.get(name.toLowerCase());
      }

      if (values != null & !values.isEmpty()) {
        return values.get(0);
      }
    }
    return null;
  }

  public static Date parseDateTimeFromString(String datetime) {
    Date date = tryToParse(datetime, RFC_822_DATE_FORMAT.get());
    if (date == null) {
      date = tryToParse(datetime, RFC_850_DATE_FORMAT.get());
    }
    if (date == null) {
      date = tryToParse(datetime, ANSI_DATE_FORMAT.get());
    }
    return date;
  }

  public static long parseDateTimeToMilliseconds(String datetime) {
    Date date = parseDateTimeFromString(datetime);
    if (date != null) {
      return date.getTime();
    }
    return 0;
  }

  public static String getGMTDatetime(Date datetime) {
    return RFC_822_DATE_FORMAT.get().format(datetime);
  }

  private static Date tryToParse(String datetime, SimpleDateFormat format) {
    try {
      return format.parse(datetime);
    } catch (ParseException e) {
      return null;
    }
  }
}
