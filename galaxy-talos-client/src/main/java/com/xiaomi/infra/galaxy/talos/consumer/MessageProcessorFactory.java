/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: yongxing@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.consumer;

public interface MessageProcessorFactory {

  /**
   * Returns a message processor to be used for processing data for a (assigned) partition.
   *
   * @return Returns a processor object.
   */
  public MessageProcessor createProcessor();
}
