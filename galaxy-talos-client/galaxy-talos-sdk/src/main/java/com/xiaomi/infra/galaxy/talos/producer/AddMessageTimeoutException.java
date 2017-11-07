/**
 * Copyright 2016, Xiaomi.
 * All rights reserved.
 * Author: yongxing@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.producer;

import com.xiaomi.infra.galaxy.talos.thrift.GalaxyTalosException;

public class AddMessageTimeoutException extends GalaxyTalosException {
  public AddMessageTimeoutException() {
    setErrMsg("addUserMessage timeout");
  }

  public AddMessageTimeoutException(String detailMsg) {
    this();
    setDetails(detailMsg);
  }
}
