/**
 * Copyright 2017, Xiaomi.
 * All rights reserved.
 * Author: xiajun@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.client.serialization;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import com.xiaomi.infra.galaxy.talos.client.Utils;
import com.xiaomi.infra.galaxy.talos.thrift.Message;
import com.xiaomi.infra.galaxy.talos.thrift.MessageType;

public class MessageSerializerV3 extends MessageSerializer {
  private static final int CREATE_TIMESTAMP_BYTES = 8;
  private static final int MESSAGE_TYPE_BYTES = 2;
  private static final int SEQUENCE_NUMBER_LENGTH_BYTES = 2;
  private static final int SCHEMA_FINGERPRINTS_LENGTH_BYTES = 2;
  private static final int MESSAGE_DATA_LENGTH_BYTES = 4;

  public static final int MESSAGE_HEADER_BYTES = VERSION_NUMBER_LENGTH +
      MESSAGE_TYPE_BYTES + CREATE_TIMESTAMP_BYTES +
      SEQUENCE_NUMBER_LENGTH_BYTES + SCHEMA_FINGERPRINTS_LENGTH_BYTES +
      MESSAGE_DATA_LENGTH_BYTES;

  private static final MessageSerializerV3 INSTANCE = new MessageSerializerV3();

  public static MessageSerializerV3 get() {
    return INSTANCE;
  }

  private MessageSerializerV3() {
  }

  @Override
  public void serialize(Message message, DataOutputStream dataOutputStream) throws IOException {
    // write version number;
    MessageSerializer.writeMessageVersion(MessageVersion.V3, dataOutputStream);

    // write message;
    byte[] messageData = Utils.serialize(message);
    dataOutputStream.writeInt(messageData.length);
    dataOutputStream.write(messageData);
  }

  @Override
  public Message deserialize(byte[] header, DataInputStream dataInputStream) throws IOException {
    int messageSize = dataInputStream.readInt();
    byte[] messageData = new byte[messageSize];
    dataInputStream.readFully(messageData);

    return Utils.deserialize(messageData, Message.class);
  }

  @Override
  public int getMessageSize(Message message) {
    return 0;
  }
}
