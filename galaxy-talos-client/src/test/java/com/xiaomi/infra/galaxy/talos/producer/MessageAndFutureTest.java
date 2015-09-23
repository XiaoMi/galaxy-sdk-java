/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: yongxing@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.producer;

import java.nio.ByteBuffer;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.xiaomi.infra.galaxy.talos.thrift.Message;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class MessageAndFutureTest {
  private static MessageAndFuture messageAndFuture;
  private static Message message;
  private static final String partitionKey = "123456qwerty";
  private static final String sequenceNumber = "654321";
  private static final ByteBuffer data = ByteBuffer.wrap("hello".getBytes());

  @Before
  public void setUp() {
    message = new Message(data).setPartitionKey(partitionKey);
    messageAndFuture = new MessageAndFuture(message);
  }

  @After
  public void tearDown() {

  }

  @Test
  public void testGetMessage() {
    assertEquals(message, messageAndFuture.getMessage());
  }

  @Test
  public void testGetFuture() {
    assertNotNull(messageAndFuture.getFuture());
  }

  @Test
  public void testGetTimestamp() {
    assertNotNull(messageAndFuture.getTimestamp());
  }

  @Test
  public void testGetMessageSize() {
    assertEquals(data.array().length, messageAndFuture.getMessageSize());

    message.setSequenceNumber(sequenceNumber);
    messageAndFuture = new MessageAndFuture(message);
    assertEquals(data.array().length + sequenceNumber.length(),
        messageAndFuture.getMessageSize());
  }
}
