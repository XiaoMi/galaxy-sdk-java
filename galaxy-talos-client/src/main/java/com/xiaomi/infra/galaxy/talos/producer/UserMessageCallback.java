/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: yongxing@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.producer;

/**
 * Note: this interface is not thread-safe
 */
public interface UserMessageCallback {

  /**
   * Implement this method to process messages that successfully put to server
   * user can get 'messageList', 'partitionId' and 'isSuccessful' by userMessageResult
   */
  public void onSuccess(UserMessageResult userMessageResult);

  /**
   * Implement this method to process messages failed to put to server
   * user can get 'messageList', 'partitionId' and 'cause' by userMessageResult
   */
  public void onError(UserMessageResult userMessageResult);
}
