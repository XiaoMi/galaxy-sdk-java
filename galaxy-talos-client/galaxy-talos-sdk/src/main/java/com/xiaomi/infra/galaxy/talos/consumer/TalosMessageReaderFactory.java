/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: yongxing@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.consumer;

public class TalosMessageReaderFactory implements MessageReaderFactory {

  @Override
  public MessageReader createMessageReader(TalosConsumerConfig consumerConfig) {
    return new TalosMessageReader(consumerConfig);
  }
}
