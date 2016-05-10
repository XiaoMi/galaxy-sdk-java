/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: yongxing@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.client;

import com.xiaomi.infra.galaxy.talos.thrift.TopicTalosResourceName;

public interface TopicAbnormalCallback {

  /**
   * User implement this method to process topic abnormal status such as 'TopicNotExist'
   */
  public void abnormalHandler(TopicTalosResourceName topicTalosResourceName,
      Throwable throwable);
}
