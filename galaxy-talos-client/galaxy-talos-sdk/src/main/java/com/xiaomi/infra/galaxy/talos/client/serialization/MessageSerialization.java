/**
 * Copyright 2016, Xiaomi.
 * All rights reserved.
 * Author: xiajun@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.client.serialization;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;

import com.google.common.base.Preconditions;

import com.xiaomi.infra.galaxy.talos.thrift.Message;

public class MessageSerialization  {
  public static void serializeMessage(Message message,
      DataOutputStream dataOutputStream,
      MessageVersion messageVersion) throws IOException {
    MessageSerializationFactory.getMessageSerializer(messageVersion).serialize(message, dataOutputStream);
  }

  public static Message deserializeMessage(DataInputStream dataInputStream) throws IOException {
    byte[] header = new byte[MessageSerializer.VERSION_NUMBER_LENGTH];
    dataInputStream.readFully(header);
    MessageVersion messageVersion = MessageSerializer.decodeMessageVersionNumber(header);

    return MessageSerializationFactory.getMessageSerializer(messageVersion)
        .deserialize(header, dataInputStream);
  }

  public static int getMessageSize(Message message, MessageVersion messageVersion) {
    return MessageSerializationFactory.getMessageSerializer(messageVersion).getMessageSize(message);
  }
}
