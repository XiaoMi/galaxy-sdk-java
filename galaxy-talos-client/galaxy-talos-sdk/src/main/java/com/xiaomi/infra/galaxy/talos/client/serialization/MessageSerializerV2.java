/**
 * Copyright 2016, Xiaomi.
 * All rights reserved.
 * Author: xiajun@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.client.serialization;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import com.xiaomi.infra.galaxy.talos.thrift.Message;

public class MessageSerializerV2 extends MessageSerializer {
  private static final int CREATE_TIMESTAMP_BYTES = 8;
  private static final int SEQUENCE_NUMBER_LENGTH_BYTES = 2;
  private static final int MESSAGE_DATA_LENGTH_BYTES = 4;

  public static final int MESSAGE_HEADER_BYTES = VERSION_NUMBER_LENGTH +
      CREATE_TIMESTAMP_BYTES + SEQUENCE_NUMBER_LENGTH_BYTES +
      MESSAGE_DATA_LENGTH_BYTES;

  private static final MessageSerializerV2 INSTANCE = new MessageSerializerV2();

  public static MessageSerializerV2 get() {
    return INSTANCE;
  }

  private MessageSerializerV2() {
  }

  @Override
  public void serialize(Message message, DataOutputStream dataOutputStream) throws IOException {
    // write version number;
    MessageSerializer.writeMessageVersion(MessageVersion.V2, dataOutputStream);

    // write createTimestamp;
    if (message.isSetCreateTimestamp()) {
      dataOutputStream.writeLong(message.getCreateTimestamp());
    } else {
      dataOutputStream.writeLong(System.currentTimeMillis());
    }


    // write sequenceNumber;
    if (message.isSetSequenceNumber()) {
      dataOutputStream.writeShort(message.getSequenceNumber().length());
      dataOutputStream.write(message.getSequenceNumber().getBytes(CHARSET));
    } else {
      dataOutputStream.writeShort(0);
    }

    // write message data;
    dataOutputStream.writeInt(message.getMessage().length);
    dataOutputStream.write(message.getMessage());
  }

  @Override
  public Message deserialize(byte[] header, DataInputStream dataInputStream) throws IOException {
    Message message = new Message();

    // read createTimestamp;
    long timestamp = dataInputStream.readLong();
    message.setCreateTimestamp(timestamp);


    // read sequence number
    short sequenceNumberSize = dataInputStream.readShort();
    if (sequenceNumberSize != 0) {
      byte[] sequenceNumberBytes = new byte[sequenceNumberSize];
      dataInputStream.readFully(sequenceNumberBytes);
      message.setSequenceNumber(new String(sequenceNumberBytes, CHARSET));
    }

    // read message data;
    int messageDataSize = dataInputStream.readInt();
    byte[] messageDataBytes = new byte[messageDataSize];
    dataInputStream.readFully(messageDataBytes);
    message.setMessage(messageDataBytes);

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
