/**
 * Copyright 2016, Xiaomi.
 * All rights reserved.
 * Author: xiajun@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.client.serialization;

public class MessageSerializationFactory {
  public static MessageSerializer getMessageSerializer(MessageVersion version) {
    switch (version) {
      case V1:
        return MessageSerializerV1.get();
      case V2:
        return MessageSerializerV2.get();
      default:
        throw new RuntimeException("Unsupported message version: " + version);
    }
  }

  public static MessageVersion getDefaultMessageVersion() {
    return MessageVersion.V2;
  }
}
