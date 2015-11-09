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

public class UserMessageTest {
  private static UserMessage userMessage;
  private static Message message;
  private static final String partitionKey = "123456qwerty";
  private static final String sequenceNumber = "654321";
  private static final ByteBuffer data = ByteBuffer.wrap("hello".getBytes());

  @Before
  public void setUp() {
    message = new Message(data).setPartitionKey(partitionKey);
    userMessage = new UserMessage(message);
  }

  @After
  public void tearDown() {

  }

  @Test
  public void testGetMessage() {
    assertEquals(message, userMessage.getMessage());
  }

  @Test
  public void testGetTimestamp() {
    assertNotNull(userMessage.getTimestamp());
  }

  @Test
  public void testGetMessageSize() {
    assertEquals(data.array().length, userMessage.getMessageSize());

    message.setSequenceNumber(sequenceNumber);
    userMessage = new UserMessage(message);
    assertEquals(data.array().length + sequenceNumber.length(),
        userMessage.getMessageSize());
  }
}
