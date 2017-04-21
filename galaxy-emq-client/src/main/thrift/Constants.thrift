/**
 * client端请求超时时间（ms）
 */
const i32 DEFAULT_CLIENT_SOCKET_TIMEOUT = 60000;

/**
 * client端连接超时时间（ms）
 */
const i32 DEFAULT_CLIENT_CONN_TIMEOUT = 30000;

/**
 * HTTP RPC服务地址
 */
const string DEFAULT_SERVICE_ENDPOINT = "http://awsbj0.emq.api.xiaomi.com";

/**
 * HTTPS RPC服务地址
 */
const string DEFAULT_SECURE_SERVICE_ENDPOINT = "https://awsbj0.emq.api.xiaomi.com";

/**
 * Queue操作RPC路径
 */
const string QUEUE_SERVICE_PATH = "/v1/api/queue";

/**
 * Message操作RPC路径
 */
const string MESSAGE_SERVICE_PATH = "/v1/api/message";

/**
 * Statistics操作RPC路径
 */
const string STATISTICS_SERVICE_PATH = "/v1/api/statistics";

const string XIAOMI_HEADER_PREFIX = "x-xiaomi-"
const string MI_DATE = "x-xiaomi-date"

const string GALAXY_ACCESS_KEY_ID = "GalaxyAccessKeyId"
const string SIGNATURE = "Signature"

const string AUTHORIZATION = "authorization"
const string CONTENT_MD5 = "content-md5"
const string CONTENT_TYPE = "content-type"
const string DATE = "date"
const string RANGE = 'range'
const string USER_AGENT = "user-agent"
const string HOST = "host"
const string TIMESTAMP = "x-xiaomi-timestamp"

const string CONTENT_LENGTH = "content-length"
