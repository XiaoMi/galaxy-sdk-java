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

public class TalosTopicKeyWritable implements Writable {
  private int partitionId;
  private long messageOffset;

  public TalosTopicKeyWritable() {
  }

  public TalosTopicKeyWritable(int partitionId, long messageOffset) {
    this.partitionId = partitionId;
    this.messageOffset = messageOffset;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    WritableUtils.writeVInt(dataOutput, partitionId);
    WritableUtils.writeVLong(dataOutput, messageOffset);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    this.partitionId = WritableUtils.readVInt(dataInput);
    this.messageOffset = WritableUtils.readVLong(dataInput);

  }

  public int getPartitionId() {
    return partitionId;
  }

  public long getMessageOffset() {
    return messageOffset;
  }
}
