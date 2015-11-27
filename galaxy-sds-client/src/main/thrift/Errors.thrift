namespace java com.xiaomi.infra.galaxy.sds.thrift
namespace php SDS.Errors
namespace py sds.errors
namespace go sds.errors

/**
 * HTTP状态码列表，用于传输层，签名错误等
 */
enum HttpStatusCode {
  /**
   * 请求格式错误，常见原因为请求参数错误导致服务端反序列化失败
   */
  BAD_REQUEST = 400,
  /**
   * 无效的认证信息，一般为签名错误
   */
  INVALID_AUTH = 401,
  /**
   * 客户端时钟不同步，服务端拒绝(为防止签名的重放攻击)
   */
  CLOCK_TOO_SKEWED = 412,
  /**
   * HTTP请求过大
   */
  REQUEST_TOO_LARGE = 413,
  /**
   * 内部错误
   */
  INTERNAL_ERROR = 500,
}

/**
 * 错误码列表，用于逻辑层错误
 */
enum ErrorCode {
  /************************
   * 系统错误
   ***********************/
  /**
   * 系统内部错误
   */
  INTERNAL_ERROR = 1,
  /**
   * 系统不可用
   */
  SERVICE_UNAVAILABLE = 2,
  /**
   * 未知错误
   */
  UNKNOWN = 3,

  END_OF_INTERNAL_ERROR = 20,

  /************************
   * 用户错误
   ***********************/
  /**
   * 无访问对应资源权限
   */
  ACCESS_DENIED = 21,
  /**
   * 无效参数
   */
  VALIDATION_FAILED = 22,
  /**
   * 长度超限(大小，数目等)
   */
  SIZE_EXCEED = 23,
  /**
   * 空间配额超限
   */
  QUOTA_EXCEED = 24,
  /**
   * 表读写配额超限
   */
  THROUGHPUT_EXCEED = 25,
  /**
   * 资源不存在(如表，应用)
   */
  RESOURCE_NOT_FOUND = 26,
  /**
   * 资源已存在(如表)
   */
  RESOURCE_ALREADY_EXISTS = 27,
  /**
   * 资源暂时不可用(如表并发管理操作加锁尚未释放)
   */
  RESOURCE_UNAVAILABLE = 28,
  /**
   * 客户端API版本不支持
   */
  UNSUPPORTED_VERSION = 29,
  /**
   * 暂时不支持的操作
   */
  UNSUPPORTED_OPERATION = 30,
  /**
   * 无效的认证信息(签名不正确，不包含签名过期)
   */
  INVALID_AUTH = 31,
  /**
   * 客户端时钟不同步
   */
  CLOCK_TOO_SKEWED = 32,
  /**
   * HTTP请求过大
   */
  REQUEST_TOO_LARGE = 33,
  /**
   * 无效请求
   */
  BAD_REQUEST = 34,
  /**
   * HTTP传输层错误
   */
  TTRANSPORT_ERROR = 35,
  /**
   * 不支持的thrift协议类型
   */
  UNSUPPORTED_TPROTOCOL = 36,
  /**
   * 请求超时
   **/
  REQUEST_TIMEOUT = 37,
}

enum RetryType {
  /**
   * 安全重试，比如建立链接超时，时钟偏移太大等错误，可以安全的进行自动重试
   */
  SAFE = 0,
  /**
   * 非安全重试，比如操作超时，系统错误等，需要开发者显式指定，系统不应自动重试
   */
  UNSAFE = 1,
}

/**
 * SDK自动重试的错误码及回退(backoff)基准时间，
 * 等待时间 = 2 ^ 重试次数 * 回退基准时间
 */
const map<ErrorCode, i64> ERROR_BACKOFF = {
  /**
   * SAFE类型
   */
  ErrorCode.SERVICE_UNAVAILABLE : 1000,
  ErrorCode.THROUGHPUT_EXCEED : 1000,
  ErrorCode.REQUEST_TIMEOUT : 0,
  ErrorCode.CLOCK_TOO_SKEWED : 0,
  /**
   * UNSAFE类型
   */
  ErrorCode.INTERNAL_ERROR : 1000,
  ErrorCode.TTRANSPORT_ERROR : 1000
},

/**
 * 错误码所对应的重试类型
 */
const map<ErrorCode, RetryType> ERROR_RETRY_TYPE = {
  ErrorCode.SERVICE_UNAVAILABLE : RetryType.SAFE,
  ErrorCode.THROUGHPUT_EXCEED : RetryType.SAFE,
  ErrorCode.REQUEST_TIMEOUT : RetryType.SAFE,
  ErrorCode.CLOCK_TOO_SKEWED : RetryType.SAFE,
  ErrorCode.INTERNAL_ERROR :  RetryType.UNSAFE,
  ErrorCode.TTRANSPORT_ERROR : RetryType.UNSAFE
},

/**
 * 抛出异常之前最大重试次数
 */
const i32 MAX_RETRY = 1,

/**
 * RPC调用错误
 */
exception ServiceException {
  /**
   * 错误码
   */
  1: optional ErrorCode errorCode,
  /**
   * 错误信息
   */
  2: optional string errorMessage,
  /**
   * 错误信息细节
   */
  3: optional string details,
  /**
   * RPC调用标识
   */
  4: optional string callId,
  /**
   * 请求标识
   */
  5: optional string requestId,
}

