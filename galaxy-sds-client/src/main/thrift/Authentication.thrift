include "Errors.thrift"
include "Common.thrift"

namespace java com.xiaomi.infra.galaxy.sds.thrift
namespace php SDS.Auth
namespace py sds.auth
namespace go sds.auth

/**
 * 小米存储系统认证信息类型
 */
enum UserType {
  /*********************
   * 开发者用户
   ********************/
  /**
   * 小米开发者用户SSO登录，此登录方式不支持签名，需要设置：
   * secretKeyId = sid (目前开发者站sid为mideveloper)，
   * secretKey = serviceToken
   */
  DEV_XIAOMI_SSO = 1,

  /**
   * 小米开发者用户，目前不支持
   */
  DEV_XIAOMI = 2,

  /*********************
   * 应用用户，需要指定appId
   ********************/
  /**
   * 小米开发者AppID, AppSecret登录
   */
  APP_SECRET = 10,

  /**
   * 存储平台签发的Storage Access Token登录，
   * 用于支持第三方认证系统(如各大OAuth系统)
   * 目前不支持
   */
  APP_ACCESS_TOKEN = 11,

  /**
   * 小米SSO登录的用户用户，此登录方式不支持签名，需要设置：
   * secretKeyId = appId，
   * secretKey = serviceToken (对应的sid为galaxysds)
   */
  APP_XIAOMI_SSO = 12,

  /**
   * 匿名登录
   */
  APP_ANONYMOUS = 13,
}

/**
 * 登录方式是否支持签名
 */
const map<UserType, bool> SIGNATURE_SUPPORT = {
  UserType.DEV_XIAOMI_SSO : false,
  UserType.DEV_XIAOMI : true,
  UserType.APP_SECRET : true,
  UserType.APP_ACCESS_TOKEN : true,
  UserType.APP_XIAOMI_SSO : false,
  UserType.APP_ANONYMOUS : false
}

/**
 * 小米存储系统认证信息
 */
struct Credential {
  /**
   * 用户登录类型
   */
  1: optional UserType type,

  /**
   * 用于服务端查询SecretKey的键值:
   * 1) userId: 对应User Secret
   * 2) appId: 对应App Secret，匿名登录也需设置
   * 3) storageAccessTokenId: 对应Storage Access Token
   */
  2: optional string secretKeyId,

  /**
   * Secret Key
   */
  3: optional string secretKey,
}

/**
 * 签名使用的HMMAC算法
 */
enum MacAlgorithm {
  HmacMD5 = 1,
  HmacSHA1 = 2,
  HmacSHA256 = 3,
}

/**
 * 兼容旧sdk
 * Authorization头包含的内容
 */
struct HttpAuthorizationHeader {
  1: optional string version = "SDS-V1",
  2: optional UserType userType = UserType.APP_ANONYMOUS,
  3: optional string secretKeyId,
  /**
   * 直接使用sercetKey，此项被设置时，signature将被忽略，
   * 非安全传输应使用签名
   */
  4: optional string secretKey,
  /**
   * 如secretKey未设置，则认为使用签名，此时必须设置，
   * 被签名的正文格式：header1[\nheader2[\nheader3[...]]]，
   * 如使用默认SUGGESTED_SIGNATURE_HEADERS时为：$host\n$timestamp\n$md5
   */
  5: optional string signature,
  /**
   * 签名HMAC算法，客户端可定制，
   * 使用签名时必须设置
   */
  6: optional MacAlgorithm algorithm,
  /**
   * 包含所有签名涉及到的部分，建议使用SUGGESTED_SIGNATURE_HEADERS，
   * 服务端未强制必须使用所列headers，定制的client自己负责签名的安全强度，
   * 使用签名时必须设置
   */
  7: optional list<string> signedHeaders = [],

  8: optional bool supportAccountKey = false,
}

/**
 * 内容为TJSONTransport.encode(HttpAuthorizationHeader)
 */
const string HK_AUTHORIZATION = "Authorization"

/**
 * 签名相关的HTTP头，
 * 根据分层防御的设计，使用HTTPS也建议进行签名:
 * http://bitcoin.stackexchange.com/questions/21732/why-api-of-bitcoin-exchanges-use-hmac-over-https-ssl
 */
const string HK_HOST = "Host"
/**
 * 签名时间，1970/0/0开始的秒数，如客户端与服务器时钟相差较大，会返回CLOCK_TOO_SKEWED错误
 */
const string HK_TIMESTAMP = "X-Xiaomi-Timestamp"
const string HK_CONTENT_MD5 = "X-Xiaomi-Content-MD5"

/**
 * 认证相关的HTTP头
 */
const string HK_VERSION = "X-Xiaomi-Version"
const string HK_USER_TYPE = "X-Xiaomi-User-Type"
const string HK_SECRET_KEY_ID = "X-Xiaomi-Secret-Key-Id"
/**
 * 直接使用secretKey，此项被设置时，signature将被忽略，
 * 非安全传输应使用签名
 */
const string HK_SECRET_KEY = "X-Xiaomi-Secret-Key"
/**
 * 如secretKey未设置，则认为使用签名，此时必须设置，
 * 被签名的正文格式：header1[\nheader2[\nheader3[...]]]，
 * 如使用默认SUGGESTED_SIGNATURE_HEADERS时为：$host\n$timestamp\n$md5
 */
const string HK_SIGNATURE = "X-Xiaomi-Signature"
/**
 * 签名HMAC算法，客户端可定制，
 * 使用签名时必须设置
 */
const string HK_MAC_ALGORITHM = "X-Xiaomi-Mac-Algorithm"
/**
 * 向后兼容，在APP_SECRET的用户身份下成为DevelopUser
 */
const string HK_SUPPORT_ACCOUNT_KEY = "X-Xiaomi-Support-Account-Key"

/**
 * 建议签名应包含的HTTP头，包含所有签名涉及到的部分，服务端未强制必须使用所列headers，
 * 定制的client自己负责签名的安全强度，使用签名时必须设置
 */
const list<string> SUGGESTED_SIGNATURE_HEADERS = [HK_HOST, HK_TIMESTAMP, HK_CONTENT_MD5]

/**
 * 第三方身份认证提供方，用于认证应用用户(非开发者)。
 * 目前提供小米SSO和几种常见OAuth系统
 */
enum AppUserAuthProvider {
  XIAOMI_SSO = 1,
  XIAOMI_OAUTH = 2,
  QQ_OAUTH = 3,
  SINA_OAUTH = 4,
  RENREN_OAUTH = 5,
  WEIXIN_OAUTH = 6,
}

/**
 * Oauth认证信息
 */
struct OAuthInfo {
  /**
   * 小米AppId
   */
  1: string xiaomiAppId,

  /**
   * 第三方身份认证提供方
   */
  2: AppUserAuthProvider appUserAuthProvider,
  /**
   * 第三方认证的accessToken
   */
  3: optional string accessToken,

  /**
   * 仅用于微信OAuth认证
   */
  4: optional string openId,

  /**
   * 仅用于小米OAuth认证
   */
  5: optional string macKey,

  /**
   * 仅用于小米OAuth认证
   */
  6: optional string macAlgorithm,
}

/**
 * 结构化存储授权相关接口(目前尚未开放)
 */
service AuthService extends Common.BaseService {
  /**
   * 通过第三方认证系统换发Storage Access Token，采用App Secret登录无需此过程
   */
  Credential createCredential(1:OAuthInfo oauthInfo) throws (1: Errors.ServiceException se),
}

