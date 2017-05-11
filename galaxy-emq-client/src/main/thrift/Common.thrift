namespace java com.xiaomi.infra.galaxy.emq.thrift
namespace php EMQ.Common
namespace py emq.common
namespace go emq.common

/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: shenyuannan@xiaomi.com
 */

exception GalaxyEmqServiceException {
  1: optional i32 errorCode;

  2: optional string errMsg;

  3: optional string details;

  4: optional string requestId;

  5: optional string queueName;
}

/**
 * List of ErrorCode.
 */
enum ErrorCode {
  /************************
   * User errors.
   ***********************/
  /**
   * Invalid action.
   */
  INVALID_ACTION = 1,
  /**
   * Invalid attributes.
   */
  INVALID_ATTRIBUTE = 2,
  /**
   * Queue has been deleted recently.
   */
  QUEUE_DELETED_RECENTLY = 3,
  /**
   * Queue already exists.
   */
  QUEUE_EXIST = 4,
  /**
   * Queue name is missing.
   */
  QUEUE_NAME_MISSING = 5,
  /**
   * Queue does not exist.
   */
  QUEUE_NOT_EXIST = 6,
  /**
   * Queue is used by others.
   */
  QUEUE_INUSE = 7,
  /**
   * Queue uri is conflict.
   */
  QUEUE_URI_CONFLICT = 8,
  /**
   * Invalid receipt handle.
   */
  INVALID_INDEX_ID = 9,
  /**
   * Message body is missing.
   */
  MESSAGE_BODY_MISSING = 10,
  /**
   * Receipt handle not exit.
   */
  INVALID_RECEIPT_HANDLE = 11,
  /**
   * Index not unique.
   */
  INDEX_NOT_UNIQUE = 12,
  /**
   * Permission denied.
   */
  PERMISSION_DENIED = 13,
  /**
    * Permission denied.
    */
  REQUEST_LENGTH_EXCEEDED = 34,

  /**
   * Bad request.
   */
  BAD_REQUEST = 35,

  /************************
   * System errors.
   ***********************/
  /**
   * System internal error.
   */
  INTERNAL_ERROR = 14,
  /**
   * Partition does not exist.
   */
  PARTITION_NOT_EXIST = 15,
  /**
   * Partition is not running.
   */
  PARTITION_NOT_RUNNING = 16,
  /**
   * Queue does not exit in cache.
   */
  QUEUE_NOT_CACHED = 17,
  /**
   * Partition is not serving.
   */
  PARTITION_NOT_SERVING = 18,
  /**
   * TTransport error, connect server error.
   */
  TTRANSPORT_ERROR = 19,
  /**
   * Quota exceeded exception.
   */
  QUOTA_EXCEEDED = 20,
  /**
   * Quota not exist exception.
   */
  QUOTA_NOT_EXIST = 21,
  /**
    * Quota lock failed exception.
    */
  QUOTA_LOCK_FAILED = 22,
  /**
   * Unknown exception.
   */
  UNKNOWN = 30,
}

enum RetryType {
  /**
   * Safe retry.
   */
  SAFE = 0,
  /**
   * Unsafe retry.
   */
  UNSAFE = 1,
  /**
   * Unsure retry, needs further determines.
   */
  UNSURE = 2,
}

/**
 * SDK auto retry ErrorCode and backOff reference time,
 * Wait time = 2 ^ retry time * backOff reference time
 */
const map<ErrorCode, i64> ERROR_BACKOFF = {
  /**
   * SAFE
   */
  ErrorCode.PARTITION_NOT_EXIST : 1000,
  ErrorCode.PARTITION_NOT_SERVING : 1000,
  ErrorCode.PARTITION_NOT_RUNNING : 1000,
  ErrorCode.QUEUE_NOT_CACHED :1000,
  ErrorCode.QUEUE_INUSE : 1000,
  /**
   * UNSAFE
   */
  ErrorCode.INTERNAL_ERROR : 1000,
  /**
   * UNSURE
   */
  ErrorCode.TTRANSPORT_ERROR : 1000,
},

/**
 * Retry types for defined ErrorCode.
 */
const map<ErrorCode, RetryType> ERROR_RETRY_TYPE = {
  /**
   * SAFE: can be auto-retry.
   */
  ErrorCode.PARTITION_NOT_EXIST : RetryType.SAFE,
  ErrorCode.PARTITION_NOT_SERVING : RetryType.SAFE,
  ErrorCode.PARTITION_NOT_RUNNING : RetryType.SAFE,
  ErrorCode.QUEUE_NOT_CACHED : RetryType.SAFE,
  ErrorCode.QUEUE_INUSE : RetryType.SAFE,
  /**
   * UNSAFE: when user set retry times, will retry.
   */
  ErrorCode.INTERNAL_ERROR : RetryType.UNSAFE,
  /**
   * UNSURE: need further determine.
   */
  ErrorCode.TTRANSPORT_ERROR : RetryType.UNSURE,
},

/**
 * The max retry time before throwing exception.
 */
const i32 MAX_RETRY = 3,

struct Version {
  /**
  * The major version number;
  **/
  1: required i32 major = 1;

  /**
  * The minor version number;
  **/
  2: required i32 minor = 0;

  /**
  * The revision number;
  **/
  3: required i32 revision = 0;

  /**
  * The date for release this version;
  **/
  4: required string date = "19700101";

  /**
  * The version details;
  **/
  5: optional string details = "";
}

service EMQBaseService {
  /**
  * Get EMQ service version;
  **/
  Version getServiceVersion() throws (1: GalaxyEmqServiceException e),

  /**
  * Check the version compatibility between client and server;
  **/
  void validClientVersion(1: Version clientVersion) throws (1: GalaxyEmqServiceException e),
}
