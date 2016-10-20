/**
 * Copyright 2016, Xiaomi.
 * All rights reserved.
 * Author: xiajun@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.client.serialization;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;

import com.xiaomi.infra.galaxy.talos.thrift.Message;

public abstract class MessageSerializer {
  public static final int VERSION_NUMBER_LENGTH = 4;
  protected static final Charset CHARSET = Charset.forName("UTF-8");

  public static MessageVersion decodeMessageVersionNumber(byte[] header) {
    if (header[0] != 'V') {
      return MessageVersion.V1;
    } else {
      int number = ((int)header[1] << 8) + header[2];
      return MessageVersion.valueOf("V" + number);
    }
  }

  public static void writeMessageVersion(MessageVersion messageVersion,
      DataOutputStream dataOutputStream) throws IOException{
    int versionNumber = messageVersion.getVersion();
    dataOutputStream.writeByte('V');
    dataOutputStream.writeShort(versionNumber);
    dataOutputStream.writeByte(0);
  }

  abstract public void serialize(Message message, DataOutputStream dataOutputStream)
      throws IOException;

  abstract public Message deserialize(byte[] header, DataInputStream dataInputStream)
      throws IOException;

  abstract public int getMessageSize(Message message);

}
