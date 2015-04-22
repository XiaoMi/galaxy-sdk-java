package com.xiaomi.infra.galaxy.rpc.client;

import com.xiaomi.infra.galaxy.rpc.thrift.ErrorCode;
import com.xiaomi.infra.galaxy.rpc.thrift.ErrorsConstants;
import com.xiaomi.infra.galaxy.rpc.thrift.RetryType;
import com.xiaomi.infra.galaxy.rpc.thrift.ServiceException;
import libthrift091.transport.TTransportException;

/**
 * Created by qiankai on 12/10/14.
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

  public static RetryType getRetryType(ErrorCode code) {
    return ErrorsConstants.ERROR_RETRY_TYPE.get(code);
  }
}
