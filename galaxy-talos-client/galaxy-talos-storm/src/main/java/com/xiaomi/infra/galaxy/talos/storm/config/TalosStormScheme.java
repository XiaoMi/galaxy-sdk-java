package com.xiaomi.infra.galaxy.talos.storm.config;

import backtype.storm.tuple.Fields;
import com.xiaomi.infra.galaxy.talos.storm.message.TalosStormMessage;
import com.xiaomi.infra.galaxy.talos.thrift.MessageAndOffset;
import com.xiaomi.infra.galaxy.talos.thrift.TopicAndPartition;

import java.io.Serializable;
import java.util.List;

/**
 * Created by jiasheng on 16-5-14.
 */
public interface TalosStormScheme extends Serializable {

    Iterable<List<Object>> generateTuples(TopicAndPartition topicPartition, MessageAndOffset msg);

    Fields getOutputFields();

}
