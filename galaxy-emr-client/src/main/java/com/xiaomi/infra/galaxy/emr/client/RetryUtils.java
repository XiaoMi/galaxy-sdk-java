package com.xiaomi.infra.galaxy.emr.client;

import com.xiaomi.infra.galaxy.rpc.thrift.ErrorsConstants;
import com.xiaomi.infra.galaxy.rpc.thrift.RetryType;
import com.xiaomi.infra.galaxy.rpc.thrift.ErrorCode;

import com.xiaomi.infra.galaxy.rpc.thrift.ServiceException;

/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: qiankai@xiaomi.com
 */

public class RetryUtils {
  public static ErrorCode getErrorCode(Throwable cause) {
    ErrorCode code = ErrorCode.UNKNOWN;
    if (cause instanceof ServiceException) {
      code = ((ServiceException)cause).getErrorCode();
    }
    return code;
  }

  public static RetryType getRetryType(ErrorCode code, String method) {
    RetryType getRetryType = ErrorsConstants.ERROR_RETRY_TYPE.get(code);
    if(getRetryType != null && getRetryType.equals(RetryType.UNSAFE)){
      if(method.startsWith("delete") || method.startsWith("change")){
        return RetryType.SAFE;
      }else{
        return RetryType.UNSAFE;
      }
    }
    return getRetryType;
  }
}