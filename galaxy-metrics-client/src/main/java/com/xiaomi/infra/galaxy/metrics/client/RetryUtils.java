package com.xiaomi.infra.galaxy.metrics.client;

import com.xiaomi.infra.galaxy.metrics.thrift.ErrorCode;
import com.xiaomi.infra.galaxy.metrics.thrift.ServiceException;
import libthrift091.transport.TTransportException;

/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: linshangquan@xiaomi.com
 */
public class RetryUtils {
  public static ErrorCode getErrorCode(Throwable cause) {
    ErrorCode code = ErrorCode.UNKNOWN;
    if (cause instanceof ServiceException) {
      ServiceException se = (ServiceException) cause;
      code = se.getErrorCode();
    } else if (cause instanceof HttpTTransportException) {
      HttpTTransportException te = (HttpTTransportException) cause;
      code = te.getErrorCode();
    } else if (cause instanceof TTransportException) {
      code = ErrorCode.TTRANSPORT_ERROR;
    }
    return code;
  }
}
