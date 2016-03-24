/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: yongxing@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.client;

import com.xiaomi.infra.galaxy.talos.thrift.CommonConstants;
import com.xiaomi.infra.galaxy.talos.thrift.ErrorCode;
import com.xiaomi.infra.galaxy.talos.thrift.GalaxyTalosException;
import com.xiaomi.infra.galaxy.talos.thrift.RetryType;

public class RetryUtils {
  public static ErrorCode getErrorCode(Throwable cause) {
    ErrorCode code = ErrorCode.UNKNOWN;
    if (cause instanceof GalaxyTalosException) {
      GalaxyTalosException gte = (GalaxyTalosException) cause;
      code = gte.getErrorCode();
    }
    return code;
  }

  public static RetryType getRetryType(ErrorCode code) {
    return CommonConstants.ERROR_RETRY_TYPE.get(code);
  }
}
