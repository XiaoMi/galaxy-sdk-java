/**
 * Copyright 2019, Xiaomi.
 * All rights reserved.
 * Author: huyumei@xiaomi.com
 */
package com.xiaomi.infra.galaxy.talos.consumer;

public class GreedyMessageReaderFactory implements MessageReaderFactory {
	@Override
	public MessageReader createMessageReader(TalosConsumerConfig consumerConfig) {
		return new GreedyMessageReader(consumerConfig);
	}
}
