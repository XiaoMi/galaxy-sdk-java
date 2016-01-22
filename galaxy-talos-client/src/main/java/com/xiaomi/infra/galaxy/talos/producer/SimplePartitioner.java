/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: yongxing@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.producer;

public class SimplePartitioner implements Partitioner {
  public SimplePartitioner() {
  }

  @Override
  public int partition(String partitionKey, int partitionNum) {
    return (partitionKey.hashCode() & 0x7FFFFFFF) % partitionNum;
  }
}
