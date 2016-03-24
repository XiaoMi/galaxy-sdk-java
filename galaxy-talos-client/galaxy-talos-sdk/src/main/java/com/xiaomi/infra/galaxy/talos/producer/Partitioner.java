/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: yongxing@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.producer;

public interface Partitioner {
  /**
   * Get partition id for specified partitionKey
   *
   * @param partitionKey
   * @param partitionNum
   * @return
   */
  public int partition(String partitionKey, int partitionNum);
}
