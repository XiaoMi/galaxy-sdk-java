package com.xiaomi.infra.galaxy.emq.client;

import com.xiaomi.infra.galaxy.emq.thrift.CommonConstants;
import com.xiaomi.infra.galaxy.emq.thrift.ErrorCode;
import com.xiaomi.infra.galaxy.emq.thrift.GalaxyEmqServiceException;
import com.xiaomi.infra.galaxy.emq.thrift.RetryType;

/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: qiankai@xiaomi.com
 */

public class RetryUtils {
  public static ErrorCode getErrorCode(Throwable cause) {
    ErrorCode code = ErrorCode.UNKNOWN;
    if (cause instanceof GalaxyEmqServiceException) {
      GalaxyEmqServiceException se = (GalaxyEmqServiceException) cause;
      code = ErrorCode.findByValue(se.getErrorCode());
    }
    return code;
  }

  public static RetryType getRetryType(ErrorCode code, String method) {
    RetryType getRetryType = CommonConstants.ERROR_RETRY_TYPE.get(code);
    if(getRetryType != null && getRetryType.equals(RetryType.UNSURE)){
      if(method.startsWith("delete") || method.startsWith("change")){
        return RetryType.SAFE;
      }else{
        return RetryType.UNSAFE;
      }
    }
    return getRetryType;
  }
}