package com.xiaomi.infra.galaxy.talos.storm.config;

import backtype.storm.tuple.Fields;
import com.google.common.base.Charsets;
import com.xiaomi.infra.galaxy.talos.thrift.MessageAndOffset;
import com.xiaomi.infra.galaxy.talos.thrift.TopicAndPartition;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by jiasheng on 16-5-17.
 */
public class DefaultTalosStormScheme implements TalosStormScheme {
    public static final String MESSAGE_STRING_SCHEME_KEY = "message";
    public static final String OFFSET_LONG_SCHEME_KEY = "offset";
    public static final String TOPIC_STRING_SCHEME_KEY = "topic";
    public static final String PARTITION_INT_SCHEME_KEY = "partition";

    @Override
    public Iterable<List<Object>> generateTuples(TopicAndPartition topicPartition, MessageAndOffset msg) {
        List<Object> tuple = new ArrayList<Object>();
        tuple.add(new String(msg.getMessage().getMessage(), Charsets.UTF_8));
        tuple.add(msg.messageOffset);
        tuple.add(topicPartition.topicName);
        tuple.add(topicPartition.partitionId);
        return Arrays.asList(tuple);
    }

    @Override
    public Fields getOutputFields() {
        return new Fields(MESSAGE_STRING_SCHEME_KEY, OFFSET_LONG_SCHEME_KEY,
                TOPIC_STRING_SCHEME_KEY, PARTITION_INT_SCHEME_KEY);
    }
}
