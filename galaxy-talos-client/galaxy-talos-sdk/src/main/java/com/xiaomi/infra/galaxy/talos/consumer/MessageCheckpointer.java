/**
 * Copyright 2016, Xiaomi.
 * All rights reserved.
 * Author: xiajun@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.consumer;

public interface MessageCheckpointer {
  /**
   * When user call checkpoint, TalosConsumer will checkpoint the latest
   * messageOffset(We just call it commitOffset) that had been delivered to user
   * by MessageProcessor. Once failover happens, the new MessageProcessor will
   * call it's interface init() with (commitOffset + 1) as the startMessageOffset,
   * and the new MessageProcessor will deliver message with offset equals
   * (commitOffset + 1) as the first message.
   * User should call this method periodically (e.g. once every 1 minutes),
   * Calling this API too frequently can slow down the application.
   * @return indicate checkpoint is success or not;
   */
  public boolean checkpoint();

  /**
   * Same as checkpoint, But use commotOffset as the checkpoint value.
   * There are some restrict for commitOffset:
   * 1. commitOffset must always greater or equal than startMessageOffset;
   * 2. commitOffset must always greater than last commitOffset;
   * 3. commitOffset must always littler or equal than latest deliver messageOffset.
   * @param commitOffset the messageOffset that to checkpoint
   * @return indicate checkpoint is success or not;
   */
  public boolean checkpoint(long commitOffset);

}
