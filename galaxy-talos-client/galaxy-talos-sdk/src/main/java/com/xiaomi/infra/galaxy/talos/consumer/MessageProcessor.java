/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: yongxing@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.consumer;

import java.util.List;

import com.xiaomi.infra.galaxy.talos.thrift.MessageAndOffset;
import com.xiaomi.infra.galaxy.talos.thrift.TopicAndPartition;

public interface MessageProcessor {

  /**
   * TalosConsumer invoke init to indicate it will deliver message to the
   * MessageProcessor instance;
   * @param topicAndPartition which topicAndPartition to consume message
   * @param startMessageOffset the messageOffset that read from talos;
   */
  public void init(TopicAndPartition topicAndPartition, long startMessageOffset);

  /**
   * User implement this method and process the messages read from Talos
   * @param messages the messages that read from talos;
   * @param messageCheckpointer you can use messageCheckpointer to checkpoint
   *                            the received messageOffset when you not use Talos
   *                            default checkpoint messageOffset.
   */
  public void process(List<MessageAndOffset> messages, MessageCheckpointer messageCheckpointer);

  /**
   * TalosConsumer invoke shutdown to indicate it will no longer deliver message
   * to the MessageProcess instance.
   * @param messageCheckpointer you can use messageCheckpointer to checkpoint
   *                            the received messageOffset when you not use Talos
   *                            default checkpoint messageOffset.
   */
  public void shutdown(MessageCheckpointer messageCheckpointer);
}
