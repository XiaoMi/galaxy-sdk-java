/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: yongxing@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.consumer;

public interface MessageReaderFactory {

  public MessageReader createMessageReader(TalosConsumerConfig consumerConfig);
}
