/**
 * Copyright 2016, Xiaomi.
 * All rights reserved.
 * Author: xiajun@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.client.serialization;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.xiaomi.infra.galaxy.talos.client.compression.ByteBufferBackedInputStream;
import com.xiaomi.infra.galaxy.talos.client.serialization.MessageSerialization;
import com.xiaomi.infra.galaxy.talos.client.serialization.MessageSerializerV1;
import com.xiaomi.infra.galaxy.talos.thrift.Message;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class SerializationV1Test {
  private ByteArrayOutputStream byteArrayOutputStream;
  private DataInputStream dataInputStream;
  private DataOutputStream dataOutputStream;

  private Message message1;
  private Message message2;
  private Message message3;
  private Message message4;

  private MessageSerializerV1 messageSerializer;

  @Before
  public void setUp() throws Exception {
    byteArrayOutputStream = new ByteArrayOutputStream(1000000);
    dataOutputStream = new DataOutputStream(byteArrayOutputStream);

    messageSerializer = MessageSerializerV1.get();

    message1 = new Message();
    message1.setCreateTimestamp(System.currentTimeMillis());
    message1.setPartitionKey("P1");
    message1.setSequenceNumber("S1");
    message1.setMessage("M1".getBytes());

    message2 = new Message();
    message2.setCreateTimestamp(System.currentTimeMillis());
    message2.setPartitionKey("P2");
    message2.setMessage("M2".getBytes());

    message3 = new Message();
    message3.setCreateTimestamp(System.currentTimeMillis());
    message3.setSequenceNumber("S3");
    message3.setMessage("M3".getBytes());

    message4 = new Message();
    message4.setCreateTimestamp(System.currentTimeMillis());
    message4.setMessage("M3".getBytes());
  }

  @Test
  public void testGetMessageSize() throws Exception {
    assertEquals(12, messageSerializer.getMessageSize(message1));
    assertEquals(10, messageSerializer.getMessageSize(message2));
    assertEquals(12, messageSerializer.getMessageSize(message3));
    assertEquals(10, messageSerializer.getMessageSize(message4));
  }

  @Test
  public void testSerializerMessage() throws Exception {
    messageSerializer.serialize(message1, dataOutputStream);
    System.out.print("buffer size:" + byteArrayOutputStream.size());

    messageSerializer.serialize(message2, dataOutputStream);
    System.out.print("buffer size:" + byteArrayOutputStream.size());

    messageSerializer.serialize(message3, dataOutputStream);
    System.out.print("buffer size:" + byteArrayOutputStream.size());

    messageSerializer.serialize(message4, dataOutputStream);
    System.out.print("buffer size:" + byteArrayOutputStream.size());

    dataOutputStream.flush();
    System.out.print("buffer size:" + byteArrayOutputStream.size());

    dataInputStream = new DataInputStream(new ByteBufferBackedInputStream(
        ByteBuffer.wrap(byteArrayOutputStream.toByteArray())));
    Message verifyMessage1 = MessageSerialization.deserializeMessage(dataInputStream);
    assertFalse(verifyMessage1.isSetCreateTimestamp());
    assertFalse(verifyMessage1.isSetPartitionKey());
    assertEquals(message1.getSequenceNumber(), verifyMessage1.getSequenceNumber());
    assertArrayEquals(message1.getMessage(), verifyMessage1.getMessage());

    Message verifyMessage2 = MessageSerialization.deserializeMessage(dataInputStream);
    assertFalse(verifyMessage2.isSetCreateTimestamp());
    assertFalse(verifyMessage2.isSetPartitionKey());
    assertFalse(verifyMessage2.isSetSequenceNumber());
    assertArrayEquals(message2.getMessage(), verifyMessage2.getMessage());

    Message verifyMessage3 = MessageSerialization.deserializeMessage(dataInputStream);
    assertFalse(verifyMessage3.isSetCreateTimestamp());
    assertFalse(verifyMessage3.isSetPartitionKey());
    assertEquals(message3.getSequenceNumber(), verifyMessage3.getSequenceNumber());
    assertArrayEquals(message3.getMessage(), verifyMessage3.getMessage());

    Message verifyMessage4 = MessageSerialization.deserializeMessage(dataInputStream);
    assertFalse(verifyMessage4.isSetCreateTimestamp());
    assertFalse(verifyMessage4.isSetPartitionKey());
    assertFalse(verifyMessage4.isSetSequenceNumber());
    assertArrayEquals(message4.getMessage(), verifyMessage4.getMessage());
  }
}
