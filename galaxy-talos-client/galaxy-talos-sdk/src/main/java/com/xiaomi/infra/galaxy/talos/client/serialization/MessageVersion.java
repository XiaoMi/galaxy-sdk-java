/**
 * Copyright 2016, Xiaomi.
 * All rights reserved.
 * Author: xiajun@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.client.serialization;

public enum  MessageVersion {
  V1(1),
  V2(2);

  private int version;

  MessageVersion(int version) {
    this.version = version;
  }

  int getVersion() {
    return version;
  }
}
