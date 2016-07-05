/**
 * Copyright 2016, Xiaomi.
 * All rights reserved.
 * Author: xiajun@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.mapreduce.input;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.xiaomi.infra.galaxy.talos.mapreduce.input.config.TalosTopicInputConfiguration;
import com.xiaomi.infra.galaxy.talos.mapreduce.input.model.TalosTopicKeyWritable;
import com.xiaomi.infra.galaxy.talos.mapreduce.input.model.TalosTopicMessageWritable;

public class TalosTopicInputFormat extends InputFormat<TalosTopicKeyWritable, TalosTopicMessageWritable> {
  @Override
  public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
    Configuration configuration = jobContext.getConfiguration();
    TalosTopicInputConfiguration config = new TalosTopicInputConfiguration(configuration);

    String partitionOffset = config.getPartitionOffset();
    String[] partitionSplitList = partitionOffset.split(",");
    if (partitionSplitList.length == 0) {
      throw new IllegalArgumentException("You must set " +
          "\"galaxy.talos.maprduce.partition.offset\" in format " +
          "\"partition1:startMessageOffset:endMessageOffset,partition2:startMessageOffset:endMessageOffset\"");
    }

    List<InputSplit> inputSplitList =
        new ArrayList<InputSplit>(partitionSplitList.length);
    for (String partitionSplit : partitionSplitList) {
      System.out.println("partition.offset: " + partitionSplit);
      String[] partitionData = partitionSplit.split(":");
      if (partitionData.length != 3) {
        throw new IllegalArgumentException("You must set " +
            "\"galaxy.talos.maprduce.partition.offset\" in format " +
            "\"partition1:startMessageOffset:endMessageOffset,partition2:startMessageOffset:endMessageOffset\"");
      }

      int partitionId;
      try {
        partitionId = Integer.valueOf(partitionData[0]);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("PartitionId must be Integer, should not be: " + partitionData[0], e);
      }

      long startMessageOffset;
      try {
        startMessageOffset = Long.valueOf(partitionData[1]);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("startMessageOffset must be Long, should not be: " + partitionData[1], e);
      }

      long endMessageOffset;
      try {
        endMessageOffset = Long.valueOf(partitionData[2]);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("endMessageOffset must be Long, should not be: " + partitionData[2], e);
      }

      TalosTopicInputSplit inputSplit = new TalosTopicInputSplit(
          config.getTopicResourceName(), partitionId, startMessageOffset, endMessageOffset);
      inputSplitList.add(inputSplit);
    }

    System.out.println("MR split: " + inputSplitList);
    return inputSplitList;
  }

  @Override
  public RecordReader<TalosTopicKeyWritable, TalosTopicMessageWritable>
  createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    System.out.println("start createRecordReader");
    return new TalosTopicMessageReader();
  }
}
