/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: yongxing@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.infra.galaxy.talos.thrift.TopicTalosResourceName;

public class SimpleTopicAbnormalCallback implements TopicAbnormalCallback {
  private static final Logger LOG = LoggerFactory.getLogger(
      SimpleTopicAbnormalCallback.class);

  @Override
  public void abnormalHandler(TopicTalosResourceName topicTalosResourceName,
      Throwable throwable) {
    LOG.error("Topic abnormal exception: " + throwable.toString() + " for: " +
        topicTalosResourceName.getTopicTalosResourceName());
  }
}
