/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: yongxing@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.consumer;

import java.util.List;

import com.xiaomi.infra.galaxy.talos.thrift.MessageAndOffset;

public interface MessageProcessor {

  /**
   * User implement this method and process the messages read from Talos
   */
  public void process(List<MessageAndOffset> messages);
}
