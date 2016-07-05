package com.xiaomi.infra.galaxy.sds.examples;

import com.xiaomi.infra.galaxy.sds.thrift.AppUserAuthProvider;
import com.xiaomi.infra.galaxy.sds.thrift.OAuthInfo;

public class ExtAppExampleMain {
  private static String appId = ""; // Your xiaomi AppId
  private static String secretKeyId = ""; // Your AppKey
  private static String secretKey = ""; // Your AppSecret
  private static String endpoint = "http://cnbj0.sds.api.xiaomi.com";
  private static String tableName = "java-test-oauth";

  private static String xiaomiAppId = ""; // Your xiaomi AppId
  private static String xiaomiAccessToken = ""; // Your xiaomi accessToken
  private static String xiaomiMacKey = ""; // Your xiaomi MacKey
  private static String xiaomiMacAlgorithm = ""; // Your xiaomi MacAlgorithm

  private static String sinaAppId = ""; // Your sina AppId
  private static String sinaAccessToken = ""; //Your sina accessToken, the external app guide the user to get the token for OAuth
  private static String renrenAppId = ""; //Your renren AppId
  private static String renrenAccessToken = ""; //Your renren accessToken, the external app guide the user to get the token for OAuth

  private static String weixinAppId = ""; // Your weixin AppId
  private static String weixinOpenId = ""; // Your wenxin OpenId
  private static String weixinAccessToken = ""; // Your weixin accessToken

  private static String qqAppId = ""; // Your qq AppId
  private static String qqAccessToken = ""; // Your qq accessToken



  public static void main(String[] args) throws Exception {

    // xiaomi app access table example
    OAuthInfo xiaomiOauthInfo = new OAuthInfo()
        .setXiaomiAppId(appId)
        .setAppUserAuthProvider(AppUserAuthProvider.XIAOMI_OAUTH)
        .setAccessToken(xiaomiAccessToken)
        .setMacKey(xiaomiMacKey)
        .setMacAlgorithm(xiaomiMacAlgorithm);
    ExtAppTableCreator tableCreator = new ExtAppTableCreator(appId, secretKeyId, secretKey, tableName,
        endpoint, xiaomiOauthInfo, xiaomiAppId);
    tableCreator.createTable();
    ExtAppTableAccessor tableAccessor = new ExtAppTableAccessor(tableName, endpoint, xiaomiOauthInfo);
    tableAccessor.accessData();

    // sina app access table example
    OAuthInfo sinaOauthInfo = new OAuthInfo()
        .setXiaomiAppId(appId)
        .setAppUserAuthProvider(AppUserAuthProvider.SINA_OAUTH)
        .setAccessToken(sinaAccessToken);
    tableCreator = new ExtAppTableCreator(appId, secretKeyId, secretKey, tableName,
        endpoint, sinaOauthInfo, sinaAppId);
    tableCreator.createTable();
    tableAccessor = new ExtAppTableAccessor(tableName, endpoint, sinaOauthInfo);
    tableAccessor.accessData();

    // renren app access table example
    OAuthInfo renrenOauthInfo = new OAuthInfo()
        .setXiaomiAppId(appId)
        .setAppUserAuthProvider(AppUserAuthProvider.RENREN_OAUTH)
        .setAccessToken(renrenAccessToken);
    tableCreator = new ExtAppTableCreator(appId, secretKeyId, secretKey, tableName, endpoint,
        renrenOauthInfo, renrenAppId);
    tableCreator.createTable();
    tableAccessor = new ExtAppTableAccessor(tableName, endpoint, renrenOauthInfo);
    tableAccessor.accessData();

    // weixin app access table example
    OAuthInfo weixinOauthInfo = new OAuthInfo()
        .setXiaomiAppId(appId)
        .setAppUserAuthProvider(AppUserAuthProvider.WEIXIN_OAUTH)
        .setOpenId(weixinOpenId)
        .setAccessToken(weixinAccessToken);
    tableCreator = new ExtAppTableCreator(appId, secretKeyId, secretKey, tableName, endpoint,
        weixinOauthInfo, weixinAppId);
    tableCreator.createTable();
    tableAccessor = new ExtAppTableAccessor(tableName, endpoint, weixinOauthInfo);
    tableAccessor.accessData();

    // qq app access table example
    OAuthInfo qqOauthInfo = new OAuthInfo()
        .setXiaomiAppId(appId)
        .setAppUserAuthProvider(AppUserAuthProvider.QQ_OAUTH)
        .setAccessToken(qqAccessToken);
    tableCreator = new ExtAppTableCreator(appId, secretKeyId, secretKey, tableName, endpoint,
        qqOauthInfo, qqAppId);
    tableCreator.createTable();
    tableAccessor = new ExtAppTableAccessor(tableName, endpoint, qqOauthInfo);
    tableAccessor.accessData();
  }
}
