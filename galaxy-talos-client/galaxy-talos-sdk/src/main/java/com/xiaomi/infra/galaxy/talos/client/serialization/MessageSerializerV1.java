/**
 * Copyright 2016, Xiaomi.
 * All rights reserved.
 * Author: xiajun@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.client.serialization;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import com.google.common.base.Preconditions;

import com.xiaomi.infra.galaxy.talos.thrift.Message;

public class MessageSerializerV1 extends MessageSerializer {
  private static final int SEQUENCE_NUMBER_LENGTH_BYTES = 4;
  private static final int MESSAGE_DATA_LENGTH_BYTES = 4;

  private static final int MESSAGE_HEADER_BYTES = SEQUENCE_NUMBER_LENGTH_BYTES +
      MESSAGE_DATA_LENGTH_BYTES;

  private static final MessageSerializerV1 INSTANCE =
      new MessageSerializerV1();

  public static MessageSerializerV1 get() {
    return INSTANCE;
  }

  private MessageSerializerV1() {
  }

  @Override
  public void serialize(Message message, DataOutputStream dataOutputStream)
      throws IOException {
    // write sequence number;
    if (message.isSetSequenceNumber()) {
      byte[] sequenceNumberBytes = message.getSequenceNumber().getBytes(CHARSET);
      dataOutputStream.writeInt(sequenceNumberBytes.length);
      dataOutputStream.write(sequenceNumberBytes);
    } else {
      dataOutputStream.writeInt(0);
    }

    // write message data;
    dataOutputStream.writeInt(message.getMessage().length);
    dataOutputStream.write(message.getMessage());

  }

  @Override
  public Message deserialize(byte[] header, DataInputStream dataInputStream) throws IOException {
    Message message = new Message();

    // generate sequence Number;
    int sequenceNumberSize = ((int)header[0] << 24) + ((int)header[1] << 16) +
        ((int)header[2] << 8) + header[3];
    // read sequence number
    if (sequenceNumberSize != 0) {
      byte[] sequenceNumberBytes = new byte[sequenceNumberSize];
      dataInputStream.readFully(sequenceNumberBytes);
      message.setSequenceNumber(new String(sequenceNumberBytes, CHARSET));
    }

    // read message data;
    int messageSize = dataInputStream.readInt();
    Preconditions.checkArgument(messageSize != 0);
    byte[] messageData = new byte[messageSize];
    dataInputStream.readFully(messageData);
    message.setMessage(messageData);

    return message;
  }

  @Override
  public int getMessageSize(Message message) {
    int size = MESSAGE_HEADER_BYTES;
    if (message.isSetSequenceNumber()) {
      size += message.getSequenceNumber().length();
    }

    size += message.getMessage().length;

    return size;
  }
}
