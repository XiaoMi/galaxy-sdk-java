include "Table.thrift"

namespace java com.xiaomi.infra.galaxy.sds.thrift
namespace php SDS.IO
namespace py sds.io
namespace go sds.io

/*************************************************************************
 * 结构化存储SLFile(SDS Log File)存储格式
 *
 * SLFile将SDS表的记录行存储成RSFile日志文件的形式(文件中只保存记录本身，不包
 * 含实体组键，主键以及索引等信息)，即属于逻辑日志(Logical logging)。SLFile
 * 日志文件可用于数据备份，传输或者直接作为离线分析的存储。
 *
 * SLFile文件的元信息为SLFileMetadata，通过TCompactProtocol进行序列化后得到的
 * 二进制数据存储在RSFileMeta.metadata域中。
 ************************************************************************/
 
/**
 * SLFile格式存储类型
 */
enum  SLFileType {
  /**************************************************************************
   * Datum Map文件存储格式
   *
   * SDS表中的每一行对应一个RSFile Record，其内容为DatumMapRecord(内容为
   * map<string, Datum>)通过TCompactProtocol进行序列化后的二进制数据。相比
   * RC_BASIC格式，优点是实现简单，且支持写入前表属性未知的情况。
   *
   * 假设一个SDS表有m条记录，那么转换为RSFile记录形式为(此处忽略文件头和EOF记录)：
   *
   * | Record(TCompactProtocol.encode(DatumMapRecord 1))   |
   * | Record(TCompactProtocol.encode(DatumMapRecord 2))   |
   * | Record(TCompactProtocol.encode(DatumMapRecord ...)) |
   * | Record(TCompactProtocol.encode(DatumMapRecord m))   |
   *************************************************************************/
   /**
    *  Datum Map文件存储格式
    */
  DATUM_MAP = 1,

  /**************************************************************************
   * 简单RCFile存储形式
   *
   * 此格式为RCFile(http://en.wikipedia.org/wiki/RCFile)的简化形式。一般情况下，
   * 存储效率高于DATUM_MAP，尤其适合离线分析只需要读物部分列的情况。目前的实现要求
   * 开始写入前必须已知表的所有属性(后续会支持)。
   *
   * 假设一个SDS表有m条记录，每个Row Group 1000条记录(注意每个行组记录数可根据记
   * 录大小，压缩/IO效率，以及缓存开销进行权衡，另外不同行组之间不需要相同)，表中
   * 定义了k个属性，那么转换为RSFile记录形式为(此处忽略文件头和EOF记录)：
   *
   * ----------------- Row Group 1:    0 -  999行 ---------------
   * | Record(TCompactProtocol.encode(RCBasicRowGroupHeader 1)) |
   * | Record(TCompactProtocol.encode(ValueList 11))            |
   * | Record(TCompactProtocol.encode(ValueList 12))            |
   * | Record(TCompactProtocol.encode(ValueList ...))           |
   * | Record(TCompactProtocol.encode(ValueList 1k))            |
   * ----------------- Row Group 2: 1000 - 1999行 ---------------
   * | Record(TCompactProtocol.encode(RCBasicRowGroupHeader 2)) |
   * | Record(TCompactProtocol.encode(ValueList 21))            |
   * | Record(TCompactProtocol.encode(ValueList 22))            |
   * | Record(TCompactProtocol.encode(ValueList ...))           |
   * | Record(TCompactProtocol.encode(ValueList 2k))            |
   * ----------------- Row Group n: xxxx - yyyy行 ---------------
   * | Record(TCompactProtocol.encode(RCBasicRowGroupHeader n)) |
   * | Record(TCompactProtocol.encode(ValueList n1))            |
   * | Record(TCompactProtocol.encode(ValueList n2))            |
   * | Record(TCompactProtocol.encode(ValueList ...))           |
   * | Record(TCompactProtocol.encode(ValueList nk))            |
   *
   * 即每个行组会生成k+1个RSFile记录。 
   *************************************************************************/
   /**
    * 简单RCFile存储形式
    */
  RC_BASIC = 2,

  // TODO Optimized RCFile
}

/**
 * DATUM_MAP文件格式元信息
 */
struct DatumMapMeta {
  /**
   * 属性id -> 属性名映射
   */
  1: optional map<i16, string> keyIdMap,
}

/**
 * RC_BASIC文件格式元信息
 */
struct RCBasicMeta {
  /**
   * 属性列表
   */
  1: optional list<string> keys,
  /**
   * 属性类型
   */
  2: optional map<string, Table.DataType> types,
}

/**
 * RC_BASIC Row Group头信息
 */
struct RCBasicRowGroupHeader {
  /**
   * 行组的记录总数，必须为正整数
   */
  1: optional i32 count,
  /**
   * 属性列组的相对偏移(相对与头信息结尾，即第一个列组offset总是为0)，
   * 属性的顺序与元信息的属性列表对应
   */
  2: optional list<i32> offset,
}

/**
 * SLFile格式存储元信息
 */
struct SLFileMeta {
  1: optional SLFileType type,
  2: optional DatumMapMeta datumMapMeta,
  3: optional RCBasicMeta rcBasicMeta,
}

/**
 * DATUM_MAP的Datum Map记录定义
 */
struct DatumMapRecord {
  /**
   * 数据部分
   */
  1: optional map<i16, Table.Datum> data,
  /**
   * 属性id -> 属性名映射，
   * 只记录当前未知的属性，即如果文件头或者前面记录已经包含某属性，则此处不再包含，
   * 此特性仅用于支持在创建文件时表schema未知的情况
   */
  2: optional map<i16, string> keyIdMap;
}

/**
 * RC_BASIC的列组定义
 */
struct ValueList {
  /**
   * 列组数据，仅当所有行对应的列无数据时才为空，而只有部分行对应的列无数据时，其对应值为NullValue
   */
  1: optional list<Table.Value> values;
}

/**
 * Datum Map记录定义，用于序列化原生的SDS表记录(例如，MapReduce中的序列化)
 */
struct DatumMap {
  /**
   * SDS记录行
   */
  1: optional Table.Dictionary data,
}
