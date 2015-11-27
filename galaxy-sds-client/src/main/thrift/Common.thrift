include "Errors.thrift"

namespace java com.xiaomi.infra.galaxy.sds.thrift
namespace php SDS.Common
namespace py sds.common
namespace go sds.common

/**
 * client端读写超时时间（ms）
 */
const double DEFAULT_CLIENT_TIMEOUT = 10000
/**
 * client端最大读写超时时间（ms）
 */
const double DEFAULT_MAX_CLIENT_TIMEOUT = 10000
/**
 * client端DDL操作超时时间（ms）
 */
const double DEFAULT_ADMIN_CLIENT_TIMEOUT = 30000
/**
 * client端连接超时时间（ms）
 */
const double DEFAULT_CLIENT_CONN_TIMEOUT = 3000
/**
 * HTTP RPC服务地址
 */
const string DEFAULT_SERVICE_ENDPOINT = 'http://sds.api.xiaomi.com'
/**
 * HTTPS RPC服务地址
 */
const string DEFAULT_SECURE_SERVICE_ENDPOINT = 'https://sds.api.xiaomi.com'
/**
 * RPC根路径
 */
const string API_ROOT_PATH = '/v1/api';
/**
 * 权限RPC路径
 */
const string AUTH_SERVICE_PATH =  '/v1/api/auth'
/**
 * 管理操作RPC路径
 */
const string ADMIN_SERVICE_PATH = '/v1/api/admin'
/**
 * 表数据访问RPC路径
 */
const string TABLE_SERVICE_PATH = '/v1/api/table'

/**
 * 开启ScanAction时， 获取操作数据的数目的key值
 */
const string SCAN_COUNT = 'count'

/**
 * 版本号，规则详见http://semver.org
 */
struct Version {
  /**
   * 主版本号，不同版本号之间不兼容
   */
  1: optional i32 major = 1,
  /**
   * 次版本号，不同版本号之间向后兼容
   */
  2: optional i32 minor = 0,
  /**
   * 构建版本号，不同版本之间互相兼容
   */
  3: optional string patch = '03a11ea1',
  /**
   * 附加信息
   */
  4: optional string comments = ''
}

/**
 * 结构化存储基础接口
 */
service BaseService {
  /**
   * 获取服务端版本
   */
  Version getServerVersion() throws (1: Errors.ServiceException se),
  /**
   * 检查版本兼容性
   */
  void validateClientVersion(1:Version clientVersion) throws (1: Errors.ServiceException se),
  /**
   * 获取服务器端当前时间，1970/0/0开始的秒数，可用作ping检查联通性
   */
  i64 getServerTime(),
}

/**
 * thrift传输协议
 */
enum ThriftProtocol {
  /**
   * TCompactProtocl
   */
  TCOMPACT = 0,
  /**
   * TJSONProtocol
   */
  TJSON = 1,
  /**
   * TBINARYProtocol
   */
  TBINARY = 2,
}


/**
 * 兼容其它SDK，等同于application/x-thrift-json
 */
const string DEFAULT_THRIFT_HEADER = 'application/x-thrift'
const string THRIFT_JSON_HEADER = 'application/x-thrift-json'
const string THRIFT_COMPACT_HEADER = 'application/x-thrift-compact'
const string THRIFT_BINARY_HEADER = 'application/x-thrift-binary'

const string THRIFT_JSON_PROTOCOL_CLASS = 'TJSONProtocol'
const string THRIFT_BINARY_PROTOCOL_CLASS = 'TBinaryProtocol'
const string THRIFT_COMPACT_PROTOCOL_CLASS = 'TCompactProtocol'

const map<ThriftProtocol, string> THRIFT_HEADER_MAP = {
  ThriftProtocol.TCOMPACT : THRIFT_COMPACT_HEADER,
  ThriftProtocol.TJSON : THRIFT_JSON_HEADER,
  ThriftProtocol.TBINARY : THRIFT_BINARY_HEADER
}

const map<ThriftProtocol, string> THRIFT_PROTOCOL_MAP = {
  ThriftProtocol.TCOMPACT : THRIFT_COMPACT_PROTOCOL_CLASS,
  ThriftProtocol.TJSON : THRIFT_JSON_PROTOCOL_CLASS,
  ThriftProtocol.TBINARY : THRIFT_BINARY_PROTOCOL_CLASS
}

const map<string, ThriftProtocol> HEADER_THRIFT_MAP = {
  THRIFT_COMPACT_HEADER : ThriftProtocol.TCOMPACT,
  THRIFT_JSON_HEADER : ThriftProtocol.TJSON,
  THRIFT_BINARY_HEADER : ThriftProtocol.TBINARY,
  DEFAULT_THRIFT_HEADER : ThriftProtocol.TJSON
}

/**
 * HTTP请求的超时时限
 */
const string HK_REQUEST_TIMEOUT = "X-Xiaomi-Request-Timeout"
/**
 * HTTP头的错误码
 */
const string HK_ERROR_CODE_HEADER = "X-Xiaomi-Error-Code"
/**
 * HTTP Body最大字节数
 */
const i32 MAX_CONTENT_SIZE = 524288
