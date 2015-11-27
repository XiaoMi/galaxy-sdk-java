include "Errors.thrift"
include "Common.thrift"
include "Authentication.thrift"
include "Table.thrift"

namespace java com.xiaomi.infra.galaxy.sds.thrift
namespace php SDS.Admin
namespace py sds.admin
namespace go sds.admin

/**
 * 应用信息
 */
struct AppInfo {
  /**
   * 小米应用ID
   */
  1: optional string appId,
  /**
   * 小米开发者ID (注意：不同于小米ID)
   */
  2: optional string developerId,
  /**
   * 表到表ID的映射
   */
  3: optional map<string, string> tableMappings,
  /**
   * 应用OAuth信息, OAuth提供方到第三方OAuth应用信息(如OAuth AppID)的映射
   */
  4: optional map<string, string> oauthAppMapping,
  /**
   * 小米应用名称
   */
  5: optional string appName,
}
/**
 * 客户端metrics的类型
 */
enum ClientMetricType {
  /**
   * 客户端请求延迟
   */
  Letency = 1,
}

/**
 * 客户端metrics数据结构
 */
struct MetricData {
  /**
   * 设置/获取metrics的类型
   */
  1: optional ClientMetricType clientMetricType,
  /**
   * 客户端请求调用的接口名称.实际计算的数据类型
   * e.g. createTable.ExecutionTime
   */
  2: optional string metricName,
  /**
   * 实际计算的数值
   */
  3: optional i64 value,
  /**
   * 客户端请求返回的时间戳
   */
  4: optional i64 timeStamp,
}

/**
 * 客户端metrics请求延迟的类型
 */
enum LatencyMetricType {
  /**
   * 客户端执行请求花费的时间
   */
  ExecutionTime = 1,
}

/**
 * 客户端用于传输metrics的数据结构
 */
struct ClientMetrics {
  /**
   * 添加/获取客户端metrics数据
   */
  1: optional list<MetricData> metricDataList,
}

/**
 * 系统统计指标类型
 */
enum MetricKey {
  /**
   * 速率类型统计指标起始
   */
  METER_METRIC_MIN = 0,
  /**
   * 限流检查通过的读操作
   */
  READ_ALLOWED = 1,
  /**
   * 限流检查拒绝的读操作
   */
  READ_THROTTLED = 2,
  /**
   * 限流检查通过的写操作
   */
  WRITE_ALLOWED = 3,
  /**
   * 限流检查拒绝的写操作
   */
  WRITE_THROTTLED = 4,
  /**
   * 成功调用
   */
  ACTION_SUCCESS = 5,
  /**
   * 客户端错误导致的失败调用
   */
  ACTION_CLIENT_ERROR = 6,
  /**
   * 系统错误导致的失败调用
   */
  ACTION_SYSTEM_ERROR = 7,
  /**
   * 速率类型统计指标结束
   */
  METER_METRIC_MAX = 49,

  /**
   * 直方图类型统计指标起始
   */
  HISTOGRAM_METRIC_MIN = 50,
  /**
   * CreateTable 调用延迟
   */
  CREATE_LATENCY = 51,
  /**
   * DropTable 调用延迟
   */
  DROP_LATENCY = 52,
  /**
   * DescribeTable 调用延迟
   */
  DESCRIBE_LATENCY = 53,
  /**
   * AlterTable 调用延迟
   */
  ALTER_LATENCY = 54,
  /**
   * EnableTable 调用延迟
   */
  ENABLE_LATENCY = 55,
  /**
   * DisableTable 调用延迟
   */
  DISABLE_LATENCY = 56,
  /**
   * QueryMetrics 调用延迟
   */
  METRICQUERY_LATENCY = 57,
  /**
   * Get 调用延迟
   */
  GET_LATENCY = 58,
  /**
   * Put 调用延迟
   */
  PUT_LATENCY = 59,
  /**
   * Increment 调用延迟
   */
  INCREMENT_LATENCY = 60,
  /**
   * Delete(Remove) 调用延迟
   */
  DELETE_LATENCY = 61,
  /**
   * Scan 调用延迟
   */
  SCAN_LATENCY = 62,
  /**
   * Batch 调用延迟
   */
  BATCH_LATENCY = 63,
  /**
   * 直方图类型统计指标结束
   */
  HISTOGRAM_METRIC_MAX = 100,
}

/**
 * 统计指标的子类型
 * (MetricKey, MetricType) 元组唯一确定一个统计指标
 */
enum MetricType {
  /**
   * 计数器，支持速率类型和直方图类型的统计指标
   */
  COUNT = 1,
  /**
   * 1分钟CPS(Count Per Second)均值，支持速率类型的统计指标
   */
  M1_RATE = 2,
  /**
   * 5分钟CPS(Count Per Second)均值，支持速率类型的统计指标
   */
  M5_RATE = 3,
  /**
   * 15分钟CPS(Count Per Second)均值，支持速率类型的统计指标
   */
  M15_RATE = 4,
  /**
   * 均值，支持直方图类型的统计指标
   */
  MEAN = 5,
  /**
   * 标准差，支持直方图类型的统计指标
   */
  STDDEV = 6,
  /**
   * 中位数，支持直方图类型的统计指标
   */
  P50 = 7,
  /**
   * 75%百分位数，支持直方图类型的统计指标
   */
  P75 = 8,
  /**
   * 95%百分位数，支持直方图类型的统计指标
   */
  P95 = 9,
  /**
   * 98%百分位数，支持直方图类型的统计指标
   */
  P98 = 10,
  /**
   * 99%百分位数，支持直方图类型的统计指标
   */
  P99 = 11,
}

/**
 * 时间间隔单位，用于查询统计指标时的下采样
 */
enum TimeSpanUnit {
  SECONDS = 1,
  MINUTES = 2,
  HOURS = 3,
}

/**
 * 统计指标查询请求
 */
struct MetricQueryRequest {
  /**
   * 需要查询的表名
   */
  1: optional string tableName,
  /**
   * 起始时间，值为1970/0/0开始的秒数
   */
  2: optional i64 startTime,
  /**
   * 结束时间，值为1970/0/0开始的秒数
   */
  3: optional i64 stopTime,
  /**
   * 统计指标主类型
   */
  4: optional MetricKey metricKey,
  /**
   * 统计指标子类型
   */
  5: optional MetricType metricType,
  /**
   * 下采样时间间隔, 0或者负数表示读取原始数据不进行下采样
   */
  6: optional i32 downsampleInterval,
  /**
   * 下采样时间间隔单位
   */
  7: optional TimeSpanUnit downsampleTimeUnit,
}

/**
 * 统计指标时间序列
 */
struct TimeSeriesData {
  /**
   * 表名
   */
  1: optional string tableName,
  /**
   * 统计指标主类型
   */
  2: optional MetricKey metricKey,
  /**
   * 统计指标子类型
   */
  3: optional MetricType metricType,
  /**
   * 统计指标数据时间序列，值为{时间 => 数值}映射
   */
  4: optional map<i64, double> data,
}

/**
 * 结构化存储管理接口
 */
service AdminService extends Common.BaseService {
  /**
   * 保存应用信息，用于注册第三方应用OAuth信息
   */
  void saveAppInfo(1: AppInfo appInfo) throws (1: Errors.ServiceException se),

  /**
   * 查询应用信息
   */
  AppInfo getAppInfo(1: string appId) throws (1: Errors.ServiceException se),

  /**
   * 查询指定用户所有应用信息
   */
  list<AppInfo> findAllApps() throws (1: Errors.ServiceException se),

  /**
   * 获取指定用户所有表信息
   */
  list<Table.TableInfo> findAllTables() throws (1: Errors.ServiceException se),

  /**
   * 创建表
   */
  Table.TableInfo createTable(1: string tableName, 2: Table.TableSpec tableSpec)
    throws (1: Errors.ServiceException se),

  /**
   * 删除表
   */
  void dropTable(1: string tableName) throws (1: Errors.ServiceException se),

  /**
   * 修改表
   */
  void alterTable(1: string tableName, 2: Table.TableSpec tableSpec)
    throws (1: Errors.ServiceException se),

  /**
   * 克隆表
   */
  void cloneTable(1: string srcName, 2: string destTable, 3: bool flushTable)
    throws (1: Errors.ServiceException se),

  /**
   * 关闭表读写操作
   */
  void disableTable(1: string tableName) throws (1: Errors.ServiceException se),

  /**
   * 打开表读写操作
   */
  void enableTable(1: string tableName) throws (1: Errors.ServiceException se),

  /**
   * 获取表定义
   */
  Table.TableSpec describeTable(1: string tableName) throws (1: Errors.ServiceException se),

  /**
   * 获取表状态等元信息
   */
  Table.TableStatus getTableStatus(1: string tableName) throws (1: Errors.ServiceException se),

  /**
   * 获取表状态
   */
  Table.TableState getTableState(1: string tableName) throws (1: Errors.ServiceException se),

  /**
   * 获取表分布信息，如用于MapReduce应用
   */
  list<Table.TableSplit> getTableSplits(1: string tableName, 2: Table.Dictionary startKey,
    3: Table.Dictionary stopKey) throws (1: Errors.ServiceException se),

  /**
   * 查询表统计指标
   */
  TimeSeriesData queryMetric(1: MetricQueryRequest query)
    throws (1: Errors.ServiceException se),

  /**
   * 批量查询表统计指标
   */
  list<TimeSeriesData> queryMetrics(1: list<MetricQueryRequest> queries)
    throws (1: Errors.ServiceException se),

  /**
   * 获取AppInfo列表,只包括appId和appName
   */
  list<AppInfo> findAllAppInfo() throws (1: Errors.ServiceException se),

  /**
   * 获取表空间大小
   */
  i64 getTableSize(1: string tableName) throws (1: Errors.ServiceException se),

  /**
   * 上传客户端metrics
   */
  void putClientMetrics(1: ClientMetrics clientMetrics) throws (1: Errors.ServiceException se),

  /**
   * 添加关注电话
   */
  void subscribePhoneAlert(1: string tableName, 2: string phoneNumber) throws (1: Errors.ServiceException se),

  /**
   * 取消关注电话
   */
  void unsubscribePhoneAlert(1: string tableName, 2: string phoneNumber) throws (1: Errors.ServiceException se),

  /**
   * 添加关注邮箱
   */
  void subscribeEmailAlert(1: string tableName, 2: string email) throws (1: Errors.ServiceException se),

  /**
   * 取消关注邮箱
   */
  void unsubscribeEmailAlert(1: string tableName, 2: string email) throws (1: Errors.ServiceException se),

  /**
   * 查看关注某个表的电话
   */
  list<string> listSubscribedPhone(1: string tableName) throws (1: Errors.ServiceException se),

  /**
   * 查看关注某个表的邮箱地址
   */
  list<string> listSubscribedEmail(1: string tableName) throws (1: Errors.ServiceException se),

  /**
   * 获取表空间历史大小
   */
  map<i64, i64> getTableHistorySize(1: string tableName, 2: i64 startDate, 3: i64 stopDate)

  /**
   * 对表注册订阅者
   */
  string subscribeTable(1: string tableName) throws (1: Errors.ServiceException se),

  /**
   * 对表取消订阅者
   */
  void unsubscribeTable(1: string tableName, 2: string subscriberId) throws (1: Errors.ServiceException se),

  /**
   * 获取表分片数量
   */
  i64 getTablePartitions(1: string tableName) throws (1: Errors.ServiceException se),

  /**
   * 订阅主集群表的增量更新（备集群接口）
   */
  void sinkTable(1: string srcTableName, 2: string destTableName, 3: string endpoint)
    throws (1: Errors.ServiceException se),

  /**
   * 取消订阅主集群表的增量更新（备集群接口）
   */
  void unsinkTable(1: string tableName) throws (1: Errors.ServiceException se),

  /**
   * 获取表分片的统计数据
   */
  Table.PartitionInfo getPartitionInfo(1: string tableName, 2: i64 partitionId)
    throws (1: Errors.ServiceException se),

  /**
   * 获取表分片某个订阅的统计数据
   */
  Table.SubscribeInfo getSubscribeInfo(1: string tableName, 2: i64 partitionId, 3: string subscriberId)
    throws (1: Errors.ServiceException se),
}

