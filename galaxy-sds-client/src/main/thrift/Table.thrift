include "Errors.thrift"
include "Common.thrift"
include "Authentication.thrift"

namespace java com.xiaomi.infra.galaxy.sds.thrift
namespace php SDS.Table
namespace py sds.table
namespace go sds.table

/**
 * 数据类型
 */
enum DataType {
  /**
   * 布尔类型
   */
  BOOL = 1,
  /**
   * 1字节整形
   */
  INT8 = 2,
  /**
   * 2字节整形
   */
  INT16 = 3,
  /**
   * 4字节整形
   */
  INT32 = 4,
  /**
   * 8字节整形
   */
  INT64 = 5,
  /**
   * 单精度浮点类型
   */
  FLOAT = 6,
  /**
   * 双精度浮点类型
   */
  DOUBLE = 7,
  /**
   * 字符串类型
   */
  STRING = 8,
  /**
   * 编码二进制类型，可用做索引属性
   */
  BINARY = 9,
  /**
   * 原生二进制类型，存储序列化开销最小，不能包含'0x00'数据，不可用做索引
   */
  RAWBINARY = 10,
  /**
   * 布尔类型集合
   */
  BOOL_SET  = 100,
  /**
   * 1字节整形集合
   */
  INT8_SET = 101,
  /**
   * 2字节整形集合
   */
  INT16_SET = 102,
  /**
   * 4字节整形集合
   */
  INT32_SET = 103,
  /**
   * 8字节整形集合
   */
  INT64_SET = 104,
  /**
   * 单精度浮点集合
   */
  FLOAT_SET = 105,
  /**
   * 双精度浮点集合
   */
  DOUBLE_SET = 106,
  /**
   * 字符串集合
   */
  STRING_SET = 107,
  /**
   * 编码二进制集合
   */
  BINARY_SET = 108,
}

/**
 * 数据值union类型
 */
union Value {
  1: optional bool boolValue,
  2: optional byte int8Value,
  3: optional i16 int16Value,
  4: optional i32 int32Value,
  5: optional i64 int64Value,
  /**
   * 用于FLOAT/DOUBLE类型
   */
  6: optional double doubleValue,
  7: optional string stringValue,
  /**
   * 用于BINARY/RAWBINARY类型
   */
  8: optional binary binaryValue,
  9: optional list<bool> boolSetValue,
  10: optional list<byte> int8SetValue,
  11: optional list<i16> int16SetValue,
  12: optional list<i32> int32SetValue,
  13: optional list<i64> int64SetValue,
  /**
   * 用于FLOAT/DOUBLE类型集合
   */
  14: optional list<double> doubleSetValue,
  15: optional list<string> stringSetValue,
  16: optional list<binary> binarySetValue,

  /**
   * null，只用于RC_BASIC存储格式
   */
  20: optional bool nullValue,
}

/**
 * 数据单元
 */
struct Datum {
  1: optional DataType type,
  2: optional Value value,
}

/**
 * 操作符类型
 */
enum OperatorType {
  /**
   * 等于
   */
  EQUAL = 1,
  /**
   * 不等于
   */
  NOT_EQUAL = 2,
  /**
   * 大于
   */
  GREATER = 3,
  /**
   * 大于等于
   */
  GREATER_OR_EQUAL = 4,
  /**
   * 小于
   */
  LESS = 5,
  /**
   * 小于等于
   */
  LESS_OR_EQUAL = 6,
}

/**
 * 吞吐量配额
 */
struct ProvisionThroughput {
  1: optional i64 readCapacity,
  2: optional i64 writeCapacity,
}

/**
 * 空间配额
 */
struct TableQuota {
  /**
   * 空间配额，单位为字节
   */
  1: optional i64 size,
}

struct KeySpec {
  1: optional string attribute,
  2: optional bool asc = true,
}

/**
 * 索引(Entity Group，主键或二级索引)的定义，为有序的属性列表
 */
typedef list<KeySpec> IndexSpec

/**
 * 属性列表，元素不能重复(PHP不支持set)
 */
typedef list<string> Attributes

/**
 * 索引数据一致性类型
 */
enum SecondaryIndexConsistencyMode {
  /**
   * 读取时判断索引一致性，此模式不能进行属性projection
   */
  LAZY = 0,
  /**
   * 写入时保持索引一致性
   */
  EAGER = 1,
  /**
   * 适用于只读数据，写入后不再修改，写入时采用LAZY方式，读
   * 取时采用EAGER方式
   */
  IMMUTABLE = 2,
}

/**
 * 局部二级索引定义
 */
struct LocalSecondaryIndexSpec {
  /**
   * 索引定义
   */
  1: optional IndexSpec indexSchema,
  /**
   * 映射的属性，仅当索引类型为Eager时才可设置
   */
  2: optional Attributes projections,
  /**
   * 索引数据一致性模式
   */
  3: optional SecondaryIndexConsistencyMode consistencyMode = SecondaryIndexConsistencyMode.LAZY,

  /**
   * 是否为唯一索引
   */
  4: optional bool unique = false,
}

/**
 * ACL模板，针对每个应用进行设置，
 * 开发者登录可以访问所拥有的表数据，无需额外设置
 */
enum CannedAcl {
  /**
   * App Secret登录读整表权限
   */
  APP_SECRET_READ = 1,
  /**
   * App Secret登录写整表权限
   */
  APP_SECRET_WRITE = 2,
  /**
   * 应用登录用户对Entity Group等于用户ID的记录的读权限，
   * 如果表没有设置Entity Group支持，此设置无效，
   * 授权后，不自动授予App Secret登录对应权限，必须单独设置
   */
  APP_USER_ENTITY_GROUP_READ = 3,
  /**
   * 应用登录用户对Entity Group等于用户ID的记录的写权限，
   * 如果表没有设置Entity Group支持，此设置无效，
   * 授权后，不自动授予App Secret登录对应权限，必须单独设置
   */
  APP_USER_ENTITY_GROUP_WRITE = 4,
  /**
   * 应用登录用户读整表权限(授权后，App Secret登录会自动拥有对应权限)
   */
  APP_USER_READ = 5,
  /**
   * 应用登录用户写整表权限(授权后，App Secret登录会自动拥有对应权限)
   */
  APP_USER_WRITE = 6,
  /**
   * 匿名用户读整表权限(授权后，App登录用户和App Secret登录用户会自动拥有对应权限)
   */
  PUBLIC_READ = 7,
  /**
   * 匿名用户写整表权限(授权后，App登录用户和App Secret登录用户会自动拥有对应权限)
   */
  PUBLIC_WRITE = 8,
}

/**
 * EntityGroup定义
 */
struct EntityGroupSpec {
  /**
   * 属性有序列表
   */
  1: optional IndexSpec attributes,
  /**
   * 是否对属性进行哈希分布:
   * 开启后表中记录按照(hash(attribute value), attribute value)大小顺序分布
   */
  2: optional bool enableHash = true,
}

/**
 * ACL设置，为appId到其配置的权限的映射
 */
typedef map<string, list<CannedAcl>> AclConf

/**
 * 表Schema设置
 */
struct TableSchema {
  /**
   * Schema版本号，仅作为输出，作为输入不需要设置
   */
  1: optional i32 version,
  /**
   * Entity group定义, 不设置表示不开启Entity Group支持。
   * 开启后自动支持应用用户表空间隔离(需配合相应权限设置),
   * 即每个应用用户将看到独立的表空间
   */
  2: optional EntityGroupSpec entityGroup,
  /**
   * 主键定义
   */
  3: IndexSpec primaryIndex,
  /**
   * 二级索引定义
   */
  4: optional map<string, LocalSecondaryIndexSpec> secondaryIndexes,
  /**
   * 属性定义
   */
  5: optional map<string, DataType> attributes,
  /**
   * 记录存活时间，单位为秒。-1表示不会自动删除
   */
  6: optional i32 ttl = -1,

  /**
   * 表初始分片数目，仅支持Entity Group开启hash分布的表，且仅在建表时起作用
   */
  7: optional i32 preSplits = 1,
}

/**
 * 表元信息
 */
struct TableMetadata {
  /**
   * 表ID
   * 仅作为输出值，作为输入时无需指定
   */
  1: optional string tableId,
  /**
   * 所有者的开发者ID，
   * 对于CreateTable/AlterTable，值不设置时，默认为当前登录用户。
   */
  2: optional string developerId,
  /**
   * 权限控制设置
   */
  3: optional AclConf appAcl,
  /**
   * 空间配额
   */
  4: optional TableQuota quota,
  /**
   * 吞吐量配额
   */
  5: optional ProvisionThroughput throughput,
  /**
   * 表备注信息
   */
  6: optional string description,
  /**
   * 是否做增量复制
   */
  7: optional bool enableReplication = false,
  /**
   * 是否支持全局有序扫描
   */
  8: optional bool scanInGlobalOrderEnabled,
}

/**
 * 表配置信息
 */
struct TableSpec {
  1: optional TableSchema schema,
  2: optional TableMetadata metadata,
}

/**
 * 表状态
 */
enum TableState {
  /**
   * 正在创建，不可操作
   */
  CREATING = 1,
  /**
   * 正在开启，不可操作
   */
  ENABLING = 2,
  /**
   * 开启状态，可读写
   */
  ENABLED = 3,
  /**
   * 正在关闭，不可操作
   */
  DISABLING = 4,
  /**
   * 关闭状态，不可读写
   */
  DISABLED = 5,
  /**
   * 正在删除，不可见
   */
  DELETING = 6,
  /**
   * 已删除，不可见
   */
  DELETED = 7,
  /**
   * 延迟删除, 可见
   */
  LAZY_DELETE = 8,
}

/**
 * 表状态信息
 */
struct TableStatus {
  /**
   * 表状态
   */
  1: optional TableState state,
  /**
   * 创建时间
   */
  2: optional i64 createTime,
  /**
   * 最近修改时间
   */
  3: optional i64 alterTime,
  /**
   * 最近统计时间
   */
  4: optional i64 statTime,
  /**
   * 占用空间统计，单位为字节
   */
  5: optional i64 size,
  /**
   * 行数统计，非即时精确值
   */
  6: optional i64 rowCount,
}

/**
 * 表信息
 */
struct TableInfo {
  1: optional string name,
  2: optional TableSpec spec,
  3: optional TableStatus status,
}

/**
 * 简单条件，用于checkAndPut/Delete操作，判定逻辑为: value operator field
 */
struct SimpleCondition {
  1: optional OperatorType operator,
  2: optional string field,
  3: optional Datum value,
  4: optional bool rowExist,
}

typedef map<string, Datum> Dictionary

/**
 * 表分片信息，包括起始和结束的row key
 */
struct TableSplit {
  1: optional Dictionary startKey,
  2: optional Dictionary stopKey,
}

/**
 * 表分片信息
 */
struct PartitionInfo {
  1: optional i64 putRequestNumber,
  2: optional i64 deleteRequestNumber,
  3: optional i64 incrementRequestNumber,
  4: optional i64 collectedEditNumber,
  5: optional i64 retrievedEditNumber,
  6: optional i64 fullScanNumber,
  7: optional i64 compactionNumber,
}

/**
 * 表分片订阅信息
 */
struct SubscribeInfo {
  1: optional i64 consumedDataNumber,
  2: optional i64 committedDataNumber,
  3: optional i64 consumedEditNumber,
  4: optional i64 committedEditNumber,
}

struct GetRequest {
  1: optional string tableName,
  /**
   * 记录主键，必须包含主键所有的属性
   */
  2: optional Dictionary keys,
  /**
   * 需要返回的属性列表，不指定表示返回所有属性
   */
  3: optional Attributes attributes
}

struct GetResult {
  1: optional Dictionary item,
}

struct PutRequest {
  1: optional string tableName,
  /**
   * 待写入的记录
   */
  2: optional Dictionary record,
  /**
   * 仅当满足指定条件时执行写入操作
   */
  3: optional SimpleCondition condition
}

struct PutResult {
  /**
   * 写入操作是否被执行(是否满足设置的条件)
   */
  1: optional bool success,
}

struct IncrementRequest {
  1: optional string tableName,
  /**
   * 待自增的记录主键
   */
  2: optional Dictionary keys,
  /**
   * 需要进行自增操作的属性，必须为整形，且不能为索引属性
   */
  3: optional Dictionary amounts
}

struct IncrementResult {
  1: optional Dictionary amounts,
}

struct RemoveRequest {
  1: optional string tableName,
  /**
   * 待删除的记录主键
   */
  2: optional Dictionary keys,
  /**
   * 待删除的属性列表，不指定表示删除整条记录。
   * 当删除部分属性时，即使所有属性均已被删除，记录仍存在，删除整条记录需要显式删除
   */
  3: optional Attributes attributes,
  /**
   * 仅当满足指定条件时执行删除操作
   */
  4: optional SimpleCondition condition,
}

struct RemoveResult {
  /**
   * 删除操作是否被执行（是否满足设置的条件）
   */
  1: optional bool success,
}

union Request {
  /**
   * 随机读操作
   */
  1: optional GetRequest getRequest,
  /**
   * 写入操作，不支持条件
   */
  2: optional PutRequest putRequest,
  /**
   * 自增操作
   */
  3: optional IncrementRequest incrementRequest,
  /**
   * 删除操作，不支持条件
   */
  4: optional RemoveRequest removeRequest,
}

enum ScanOp {
  /**
   * 统计满足查询条件的记录数
   */
  COUNT,
  /**
   * 删除满足查询条件的记录
   */
  DELETE,
  /**
   * 更新满足条件的记录
   */
  UPDATE,
}

struct ScanAction {
  /**
   * scan时连带操作
   */
  1: optional ScanOp action,
  /**
   * 实际操作，不需要指定key
   */
  2: optional Request request,
}

/**
 * 范围查询，支持主键和二级索引查询，
 * 查询范围为闭开区间[startKey, endKey)，
 * 当指定索引时，查询范围的entity group必须唯一指定
 */
struct ScanRequest {
  1: optional string tableName,
  /**
   * 不指定表示通过主键进行查询
   */
  2: optional string indexName,
  /**
   * 查询范围开始，包含startKey，
   * 如果startKey不是完整键，而是部分key的前缀，则实际查询的startKey为{startKey, 最小可能的后缀}补全形式
   */
  3: optional Dictionary startKey,
  /**
   * 查询范围结束，不包含stopKey，
   * 如果stopKey不是完整键，而是部分key的前缀，则实际查询的stopKey为{stopKey, 最大可能的后缀}补全形式
   */
  4: optional Dictionary stopKey,
  /**
   * 需要返回的属性列表，不指定表示返回所有属性
   */
  5: optional Attributes attributes,
  /**
   * 类SQL WHERE语句的查询条件。
   * 注意：与SQL不同，此条件仅作为过滤条件，不影响具体查询计划(index, startKey, endKey)，
   * 进行范围查询时需要显示设置index和startKey以及endKey。每个扫描的记录均计入读配额，
   * 即使不满足查询条件。尽量避免使用条件过滤，尤其是当过滤掉的记录占一半以上时，强烈不建议使用。
   */
  6: optional string condition,
  /**
   * 返回记录的最大数目，返回数目可能小于此值(如超出表的读配额时)
   */
  7: optional i32 limit = 10,
  /**
   * 是否进行逆序扫描，进行逆序扫描时startKey应大于endKey，
   * 注意：逆序查询效率较低，谨慎使用，建议设置对应的Key为逆序存储
   */
  8: optional bool reverse = false,

  /**
   * 是否全局有序扫描
   */
  9: optional bool inGlobalOrder = false,

  /**
   * 是否将结果放入cache，对于类似MapReduce的大批量扫描的应用应该关闭此选项
   */
  10: optional bool cacheResult = true,

  /**
   * 查找属性在seek之前进行顺序skip的次数。非必要情况，请不要设置
   */
  11: optional i32 lookAheadStep = 0,

  /**
   * scan时的连带操作，包括COUNT，DELETE和UPDATE
   */
  12: optional ScanAction action,

  /**
   * 扫描表分片的索引，对salted table全局无序扫描时设置
   */
  13: optional i32 splitIndex = -1;

  /**
   * 查询范围开始的初始值，对salted table全局无序扫描时设置
   */
  14: optional Dictionary initialStartKey;
}

struct ScanResult {
  /**
   * 下一个需要扫描的记录主键，NULL表示达到制定的结束位置
   */
  1: optional Dictionary nextStartKey,
  /**
   * 扫描的记录
   */
  2: optional list<Dictionary> records,
  /**
   * 是否超过表的qps quota
   */
  3: optional bool throttled,
  /**
   * 下一个需要扫描的分片索引，-1表示已经扫描完所有分片，对salted table全局无序扫描时使用
   */
  4: optional i32 nextSplitIndex,
}

enum BatchOp {
  GET = 1,
  PUT = 2,
  INCREMENT = 3,
  REMOVE = 4,
}

struct BatchRequestItem {
  1: optional BatchOp action,
  2: optional Request request,
}

union Result {
  1: optional GetResult getResult,
  2: optional PutResult putResult,
  3: optional IncrementResult incrementResult,
  4: optional RemoveResult removeResult,
}

struct BatchResultItem {
  /**
   * 操作类型
   */
  1: optional BatchOp action,
  /**
   * 是否成功执行，即无异常
   */
  2: optional bool success,
  /**
   * 操作结果，操作成功时被设置
   */
  3: optional Result result,
  /**
   * 操作时发生的异常，操作失败时被设置
   */
  4: optional Errors.ServiceException serviceException,
}

struct BatchRequest {
  1: optional list<BatchRequestItem> items,
}

struct BatchResult {
  1: optional list<BatchResultItem> items,
}

/**
 * 增量操作类型
 */
enum EditType {
  PUT = 1,
  DELETE = 2,
}

/**
 * 增量操作单元
 */
struct EditDatum {
  /**
   * 增量操作类型
   */
  1: optional EditType editType,
  /**
   * 增量操作单元的数据
   */
  2: optional Datum datum,
}

typedef map<string, EditDatum> EditDictionary

/**
 * 行级别的增量操作
 */
struct RowEdit {
  /**
   * 增量操作行的主键
   */
  1: optional Dictionary keys,
  /**
   * 增量操作行的属性
   */
  2: optional EditDictionary edits,
  /**
   * 增量偏移
   */
  3: optional i64 consumeOffset;
}

/**
 * 存量数据的消费请求
 */
struct DataConsumeRequest {
  /**
   * 表名
   */
  1: optional string tableName,
  /**
   * 表分片ID
   */
  2: optional i64 partitionId,
  /**
   * 订阅者ID
   */
  3: optional string subscriberId,
  /**
   * 消费数量
   */
  4: optional i32 consumeNumber,
  /**
   * 消费偏移
   */
  5: optional Dictionary consumeOffset,
}

struct DataConsumeResult {
  /**
   * 下一个开始消费的存量数据的偏移，NULL表示达到当前表分片的结束位置
   */
  1: optional Dictionary nextConsumeOffset,
  /**
   * 消费的存量数据
   */
  2: optional list<Dictionary> records,
  /**
   * 表的主键属性
   */
  3: optional list<string> keys,
  /**
   * 是否超过表的qps quota
   */
  4: optional bool throttled,
}

/**
 * 增量数据的消费请求
 */
struct EditConsumeRequest {
  /**
   * 表名
   */
  1: optional string tableName,
  /**
   * 表分片ID
   */
  2: optional i64 partitionId,
  /**
   * 订阅者ID
   */
  3: optional string subscriberId,
  /**
   * 消费数量
   */
  4: optional i32 consumeNumber,
  /**
   * 消费偏移
   */
  5: optional i64 consumeOffset = -1,
}

struct EditConsumeResult {
  /**
   * 下一个开始消费的增量数据的偏移，NULL表示达到当前表分片的结束位置
   */
  1: optional i64 nextConsumeOffset,
  /**
   * 消费的增量数据
   */
  2: optional list<RowEdit> rowEdits,
  /**
   * 是否超过表的qps quota
   */
  3: optional bool throttled,
}

/**
 * 存量数据的消费请求回执
 */
struct DataCommitRequest {
  /**
   * 表名
   */
  1: optional string tableName,
  /**
   * 表分片ID
   */
  2: optional i64 partitionId,
  /**
   * 订阅者ID
   */
  3: optional string subscriberId,
  /**
   * 当前消费存量数据的最后偏移
   */
  4: optional Dictionary lastConsumedOffset,
  /**
   * 确认消费数量
   */
  5: optional i32 commitNumber,
}

struct DataCommitResult {
  /**
   * 消费请求回执是否被服务器成功接收
   */
  1: optional bool success,
}

/**
 * 增量数据的消费请求回执
 */
struct EditCommitRequest {
  /**
   * 表名
   */
  1: optional string tableName,
  /**
   * 表分片ID
   */
  2: optional i64 partitionId,
  /**
   * 订阅者ID
   */
  3: optional string subscriberId,
  /**
   * 当前消费增量数据的最后偏移
   */
  4: optional i64 lastConsumedOffset,
  /**
   * 确认消费数量
   */
  5: optional i32 commitNumber,
}

struct EditCommitResult {
  /**
   * 消费请求回执是否被服务器成功接收
   */
  1: optional bool success,
}

/**
 * 结构化存储表数据访问接口
 */
service TableService extends Common.BaseService {
  /**
   * 读操作，需要1个读配额
   */
  GetResult get(1: GetRequest request) throws (1: Errors.ServiceException se),

  /**
   * 写操作，需要1个写配额，另外每个Eager二级索引需要1个额外读配额
   */
  PutResult put(1: PutRequest request) throws (1: Errors.ServiceException se),

  /**
   * 自增操作，需要读写配额各1
   */
  IncrementResult increment(1: IncrementRequest request) throws (1: Errors.ServiceException se),

  /**
   * 删除操作，需要1个写配额，另外每个Eager二级索引需要1个额外读配额
   */
  RemoveResult remove(1: RemoveRequest request) throws (1: Errors.ServiceException se),

  /**
   * 扫描操作，每个扫描过的记录消耗1个读配额(即使不满足过滤条件)，每个Lazy二级索引需要1个额外读配额
   */
  ScanResult scan(1: ScanRequest request) throws (1: Errors.ServiceException se),

  /**
   * 批量读写操作，消耗各自对应的读写配额。同一个batch中多个操作修改同一行数据可能导致未定义行为（数据不一致），
   * 应当避免，另外如果一个batch包含同一行的读和写操作，其执行顺序是不确定的，不推荐使用
   */
  BatchResult batch(1: BatchRequest request) throws (1: Errors.ServiceException se),

  /**
   * 存量数据的消费操作
   */
  DataConsumeResult consumePartitionData(1: DataConsumeRequest request) throws (1: Errors.ServiceException se),

  /**
   * 增量数据的消费操作
   */
  EditConsumeResult consumePartitionEdit(1: EditConsumeRequest request) throws (1: Errors.ServiceException se),

  /**
   * 存量数据的消费回执操作
   */
  DataCommitResult commitConsumedPartitionData(1: DataCommitRequest request)
    throws (1: Errors.ServiceException se),

  /**
   * 增量数据的消费回执操作
   */
  EditCommitResult commitConsumedPartitionEdit(2: EditCommitRequest request)
    throws (1: Errors.ServiceException se),
}
