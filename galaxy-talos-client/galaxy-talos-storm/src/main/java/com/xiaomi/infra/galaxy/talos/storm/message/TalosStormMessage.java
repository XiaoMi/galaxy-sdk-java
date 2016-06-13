package com.xiaomi.infra.galaxy.talos.storm.message;

import com.google.common.base.Joiner;
import com.xiaomi.infra.galaxy.talos.thrift.MessageAndOffset;
import com.xiaomi.infra.galaxy.talos.thrift.TopicAndPartition;

/**
 * Created by jiasheng on 16-5-16.
 */
public class TalosStormMessage {
    public final TopicAndPartition topicPartition;
    public final MessageAndOffset message;
    public final String id;


    public TalosStormMessage(TopicAndPartition topicPartition, MessageAndOffset message) {
        this.topicPartition = topicPartition;
        this.message = message;
        id = Joiner.on("-").join(topicPartition.topicName, topicPartition.partitionId, message.messageOffset);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }
        TalosStormMessage that = (TalosStormMessage) obj;
        return this.id.equals(that.id);
    }

    @Override
    public int hashCode() {
        return this.id.hashCode();
    }

}
