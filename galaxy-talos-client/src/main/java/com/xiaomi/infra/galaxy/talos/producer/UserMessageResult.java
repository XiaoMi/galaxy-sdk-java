/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: yongxing@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.producer;

public class UserMessageResult {
  private int partitionId;
  private boolean successful;

  public UserMessageResult(int partitionId, boolean successful) {
    this.partitionId = partitionId;
    this.successful = successful;
  }

  public int getPartitionId() {
    return partitionId;
  }

  public boolean isSuccessful() {
    return successful;
  }
}
