/**
 * Copyright 2016, Xiaomi.
 * All rights reserved.
 * Author: xiajun@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.mapreduce.input.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import com.xiaomi.infra.galaxy.talos.thrift.Message;

public class TalosTopicMessageWritable implements Writable {
  private Message message;

  public TalosTopicMessageWritable() {
  }

  public TalosTopicMessageWritable(Message message) {
    this.message = message;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    if (!message.isSetSequenceNumber()) {
      message.setSequenceNumber("");
    }

    WritableUtils.writeCompressedString(dataOutput, message.getSequenceNumber());
    WritableUtils.writeCompressedByteArray(dataOutput, message.getMessage());
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    message.setSequenceNumber(WritableUtils.readString(dataInput));
    message.setMessage(WritableUtils.readCompressedByteArray(dataInput));
  }
}
