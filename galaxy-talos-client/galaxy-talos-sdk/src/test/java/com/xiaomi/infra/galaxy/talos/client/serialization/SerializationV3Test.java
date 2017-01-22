/**
 * Copyright 2017, Xiaomi.
 * All rights reserved.
 * Author: xiajun@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.client.serialization;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;

import org.junit.Before;
import org.junit.Test;

import com.xiaomi.infra.galaxy.talos.client.compression.ByteBufferBackedInputStream;
import com.xiaomi.infra.galaxy.talos.thrift.Message;
import com.xiaomi.infra.galaxy.talos.thrift.MessageType;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class SerializationV3Test {
  private ByteArrayOutputStream byteArrayOutputStream;
  private DataInputStream dataInputStream;
  private DataOutputStream dataOutputStream;

  private Message message1;
  private Message message2;
  private Message message3;
  private Message message4;

  private MessageSerializerV3 messageSerializer;

  @Before
  public void setUp() throws Exception {
    byteArrayOutputStream = new ByteArrayOutputStream(1000000);
    dataOutputStream = new DataOutputStream(byteArrayOutputStream);


    messageSerializer = MessageSerializerV3.get();

    message1 = new Message();
    message1.setCreateTimestamp(System.currentTimeMillis());
    message1.setMessageType(MessageType.BINARY);
    message1.setPartitionKey("P1");
    message1.setSequenceNumber("S1");
    message1.setMessage("M1".getBytes());

    message2 = new Message();
    message2.setCreateTimestamp(System.currentTimeMillis());
    message2.setMessageType(MessageType.BINARY);
    message2.setPartitionKey("P2");
    message2.setMessage("M2".getBytes());

    message3 = new Message();
    message3.setCreateTimestamp(System.currentTimeMillis());
    message3.setMessageType(MessageType.BINARY);
    message3.setSequenceNumber("S3");
    message3.setMessage("M3".getBytes());

    message4 = new Message();
    message4.setCreateTimestamp(System.currentTimeMillis());
    message4.setMessageType(MessageType.BINARY);
    message4.setMessage("M4".getBytes());
    message4.setSchemaFingerprint("S4");
  }

  @Test
  public void testSerializerMessage() throws Exception {
    messageSerializer.serialize(message1, dataOutputStream);
    System.out.print("buffer size:"  + byteArrayOutputStream.size());

    messageSerializer.serialize(message2, dataOutputStream);
    System.out.print("buffer size:"  + byteArrayOutputStream.size());

    messageSerializer.serialize(message3, dataOutputStream);
    System.out.print("buffer size:"  + byteArrayOutputStream.size());

    messageSerializer.serialize(message4, dataOutputStream);
    System.out.print("buffer size:"  + byteArrayOutputStream.size());

    dataOutputStream.flush();
    System.out.print("buffer size:"  + byteArrayOutputStream.size());

    dataInputStream = new DataInputStream(new ByteBufferBackedInputStream(
        ByteBuffer.wrap(byteArrayOutputStream.toByteArray())));
    Message verifyMessage1 = MessageSerialization.deserializeMessage(dataInputStream);
    assertEquals(message1, verifyMessage1);

    Message verifyMessage2 = MessageSerialization.deserializeMessage(dataInputStream);
    assertEquals(message2, verifyMessage2);

    Message verifyMessage3 = MessageSerialization.deserializeMessage(dataInputStream);
    assertEquals(message3, verifyMessage3);

    Message verifyMessage4 = MessageSerialization.deserializeMessage(dataInputStream);
    assertEquals(message4, verifyMessage4);
  }
}
