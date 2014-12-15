package com.xiaomi.infra.galaxy.sds.client.examples;

import com.xiaomi.infra.galaxy.sds.thrift.AppUserAuthProvider;

public class ExtAppExampleMain {
  private static String appId = ""; // Your xiaomi AppId
  private static String secretKeyId = ""; // Your AppKey
  private static String secretKey = ""; // Your AppSecret
  private static String endpoint = "https://cnbj-s0.sds.api.xiaomi.com";
  private static String tableName = "java-test-weather";
  private static String sinaAppId = ""; // Your sina AppId
  private static String sinaAccessToken = ""; //Your sina accessToken, the external app guide the user to get the token for OAuth
  private static String renrenAppId = ""; //Your renren AppId
  private static String renrenAccessToken = ""; //Your renren accessToken, the external app guide the user to get the token for OAuth

  public static void main(String[] args) throws Exception {
    // sina app access table example
    ExtAppTableCreator tableCreator = new ExtAppTableCreator(appId, secretKeyId, secretKey, tableName,
        endpoint, AppUserAuthProvider.SINA_OAUTH, sinaAppId);
    tableCreator.createTable();
    ExtAppTableAccessor tableAccessor = new ExtAppTableAccessor(appId, tableName, endpoint,
        AppUserAuthProvider.SINA_OAUTH, sinaAccessToken);
    tableAccessor.accessData();

    // renren app access table example
    tableCreator = new ExtAppTableCreator(appId, secretKeyId, secretKey, tableName, endpoint,
        AppUserAuthProvider.RENREN_OAUTH, renrenAppId);
    tableCreator.createTable();
    tableAccessor = new ExtAppTableAccessor(appId, tableName, endpoint,
        AppUserAuthProvider.RENREN_OAUTH, renrenAccessToken);
    tableAccessor.accessData();
  }
}
