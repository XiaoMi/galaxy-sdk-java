/**
 * Copyright 2016, Xiaomi.
 * All rights reserved.
 * Author: xiajun@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.mapreduce.input;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.InputSplit;

public class TalosTopicInputSplit extends InputSplit implements Writable {
  private String topicResrouceName;
  private int parititonId;
  private long startMessageOffset;
  private long endMessageOffset;

  public TalosTopicInputSplit() {
  }

  public TalosTopicInputSplit(String topicResrouceName, int parititonId,
      long startMessageOffset, long endMessageOffset) {
    this.topicResrouceName = topicResrouceName;
    this.parititonId = parititonId;
    this.startMessageOffset = startMessageOffset;
    this.endMessageOffset = endMessageOffset;

    System.out.println("Split: " + toString());
  }

  @Override
  public long getLength() throws IOException, InterruptedException {
    return endMessageOffset - startMessageOffset;
  }

  @Override
  public String[] getLocations() throws IOException, InterruptedException {
    return new String[0];
  }

  public String getTopicResrouceName() {
    return topicResrouceName;
  }

  public int getParititonId() {
    return parititonId;
  }

  public long getStartMessageOffset() {
    return startMessageOffset;
  }

  public long getEndMessageOffset() {
    return endMessageOffset;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    WritableUtils.writeCompressedString(dataOutput, topicResrouceName);
    WritableUtils.writeVInt(dataOutput, parititonId);
    WritableUtils.writeVLong(dataOutput, startMessageOffset);
    WritableUtils.writeVLong(dataOutput, endMessageOffset);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    topicResrouceName = WritableUtils.readCompressedString(dataInput);
    parititonId = WritableUtils.readVInt(dataInput);
    startMessageOffset = WritableUtils.readVLong(dataInput);
    endMessageOffset = WritableUtils.readVLong(dataInput);
  }

  @Override
  public String toString() {
    return "TalosTopicInputSplit{" +
        "topicResrouceName='" + topicResrouceName + '\'' +
        ", parititonId=" + parititonId +
        ", startMessageOffset=" + startMessageOffset +
        ", endMessageOffset=" + endMessageOffset +
        '}';
  }
}
