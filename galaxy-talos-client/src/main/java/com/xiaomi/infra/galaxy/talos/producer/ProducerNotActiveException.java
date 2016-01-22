/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: yongxing@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.producer;

import com.xiaomi.infra.galaxy.talos.thrift.GalaxyTalosException;

public class ProducerNotActiveException extends GalaxyTalosException {
  public ProducerNotActiveException() {
    setErrMsg("producer not active");
  }

  public ProducerNotActiveException(String detailMsg) {
    this();
    setDetails(detailMsg);
  }
}
