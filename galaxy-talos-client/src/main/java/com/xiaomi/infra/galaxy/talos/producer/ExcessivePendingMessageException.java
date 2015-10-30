/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: yongxing@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.producer;

import com.xiaomi.infra.galaxy.talos.thrift.ErrorCode;
import com.xiaomi.infra.galaxy.talos.thrift.GalaxyTalosException;

public class ExcessivePendingMessageException extends GalaxyTalosException {
  public ExcessivePendingMessageException() {
    setErrorCode(ErrorCode.EXCESSIVE_PENDING_MESSAGE);
    setErrMsg("Excessive pending message");
  }

  public ExcessivePendingMessageException(String detailMsg) {
    this();
    setDetails(detailMsg);
  }
}
