/**
 * Copyright 2016, Xiaomi.
 * All rights reserved.
 * Author: xiajun@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.mapreduce.example;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.xiaomi.infra.galaxy.rpc.thrift.Credential;
import com.xiaomi.infra.galaxy.rpc.thrift.UserType;
import com.xiaomi.infra.galaxy.talos.client.TalosClientConfigKeys;
import com.xiaomi.infra.galaxy.talos.client.Utils;
import com.xiaomi.infra.galaxy.talos.consumer.SimpleConsumer;
import com.xiaomi.infra.galaxy.talos.consumer.TalosConsumerConfig;
import com.xiaomi.infra.galaxy.talos.mapreduce.input.TalosTopicInputFormat;
import com.xiaomi.infra.galaxy.talos.mapreduce.input.TalosTopicMapper;
import com.xiaomi.infra.galaxy.talos.mapreduce.input.model.TalosTopicKeyWritable;
import com.xiaomi.infra.galaxy.talos.mapreduce.input.model.TalosTopicMessageWritable;
import com.xiaomi.infra.galaxy.talos.thrift.MessageAndOffset;
import com.xiaomi.infra.galaxy.talos.thrift.TopicAndPartition;
import com.xiaomi.infra.galaxy.talos.thrift.TopicTalosResourceName;

public class TalosMessageCount {
  public static class TalosMessageCountMapper extends
      TalosTopicMapper<IntWritable, IntWritable> {
    private static final IntWritable one = new IntWritable(1);

    @Override
    protected void map(TalosTopicKeyWritable key, TalosTopicMessageWritable value,
        Context context) throws IOException, InterruptedException {
      context.write(new IntWritable(key.getPartitionId()), one);
    }
  }

  public static class TalosMessageCountReducer
      extends Reducer<IntWritable, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable intWritable : values) {
        sum += intWritable.get();
      }

      context.write(new Text("Parititon:" + key.get()), new IntWritable(sum));
      System.out.println("Partition: " + key.get() + " have number: " + sum);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    // Use GenericOptionsParse, supporting -D -conf etc.
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 1) {
      System.err.println("Usage: wordcount <out>");
      System.exit(2);
    }
    String output = otherArgs[0];
    System.out.println("Running framework: " + conf.get("mapreduce.framework.name"));
    System.out.println("File system: " + conf.get("fs.default.name"));
    final FileSystem fs = FileSystem.get(conf);
    if (conf.getBoolean("cleanup-output", true)) {
      fs.delete(new Path(output), true);
    }

    conf.set("mapreduce.task.profile.reduces", "1"); // no reduces
    Job job = new Job(conf, "CodeLab-TalosMessageCount");
    job.setJarByClass(TalosMessageCount.class);

    // setInputFormat related;
    job.setInputFormatClass(TalosTopicInputFormat.class);

    // set mapper related;
    job.setMapperClass(TalosMessageCountMapper.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(IntWritable.class);

    // set reducer related;
    job.setReducerClass(TalosMessageCountReducer.class);


    // set outputFormat related;
    FileOutputFormat.setOutputPath(job, new Path(output));
//    job.setOutputFormatClass(FileOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    try {
      job.waitForCompletion(true);
    } catch (NullPointerException e) {
      e.printStackTrace(System.out);
      e.printStackTrace();
    }

    System.out.println("job finished");
  }
}
