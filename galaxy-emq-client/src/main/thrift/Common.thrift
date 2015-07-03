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
}

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
