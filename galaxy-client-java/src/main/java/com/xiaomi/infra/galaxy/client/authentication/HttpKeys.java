package com.xiaomi.infra.galaxy.client.authentication;

public class HttpKeys {

  public static final String XIAOMI_HEADER_PREFIX = "x-xiaomi-";
  public static final String XIAOMI_META_HEADER_PREFIX =
      XIAOMI_HEADER_PREFIX + "meta-";
  public static final String ESTIMATED_OBJECT_SIZE = XIAOMI_HEADER_PREFIX
      + "estimated-object-size";

  // Required query parameters for pre-signed uri
  public static final String GALAXY_ACCESS_KEY_ID = "GalaxyAccessKeyId";
  public static final String SIGNATURE = "Signature";
  public static final String EXPIRES = "Expires";
  public static final String AUTHENTICATION = "Authentication";

  // Http headers used for authentication
  public static final String AUTHORIZATION = "authorization";
  public static final String CONTENT_MD5 = "content-md5";
  public static final String CONTENT_TYPE = "content-type";
  public static final String DATE = "date";

  public static final String CONTENT_LENGTH = "content-length";

  public static final int REQUEST_TIME_LIMIT = 15 * 60 * 1000;

  // Predefined xiaomi headers
  public static final String MI_DATE= XIAOMI_HEADER_PREFIX + "date";
  public static final String REQUEST_ID = XIAOMI_HEADER_PREFIX + "request-id";
  public static final String ACL = XIAOMI_HEADER_PREFIX + "acl";
  public static final String MI_CONTENT_LENGTH = XIAOMI_META_HEADER_PREFIX +
      "content-length";
}
