package com.xiaomi.infra.galaxy.talos.storm.message;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.xiaomi.infra.galaxy.talos.consumer.MessageCheckpointer;
import com.xiaomi.infra.galaxy.talos.consumer.MessageProcessor;
import com.xiaomi.infra.galaxy.talos.storm.config.TalosStormConfig;
import com.xiaomi.infra.galaxy.talos.thrift.MessageAndOffset;
import com.xiaomi.infra.galaxy.talos.thrift.TopicAndPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by jiasheng on 16-5-31.
 */
public class TalosStormMessageProcessor implements MessageProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(TalosStormMessageProcessor.class);
    private TalosStormConfig talosConfig;
    private TopicAndPartition topicPartition;
    private PartitionMessagePool msgPool;
    private MessageCheckpointer msgCheckpointer;
    private ConcurrentHashMap<String, TalosStormMessageProcessor> msgPools;

    public TalosStormMessageProcessor(TalosStormConfig talosConfig,
                                      ConcurrentHashMap<String, TalosStormMessageProcessor> msgPools) {
        this.talosConfig = talosConfig;
        this.msgPools = msgPools;
    }

    public Optional<PartitionMessagePool> getMessagePool() {
        return Optional.fromNullable(msgPool);
    }

    public Optional<MessageCheckpointer> getMessageCheckpointer() {
        return Optional.fromNullable(msgCheckpointer);
    }

    public Optional<TopicAndPartition> getTopicPartition() {
        return Optional.fromNullable(topicPartition);
    }

    private String getKeyString(TopicAndPartition topicPartition) {
        return Joiner.on("-").join(topicPartition.topicName, topicPartition.partitionId);
    }

    @Override
    public void init(TopicAndPartition topicAndPartition, long startMessageOffset) {
        this.topicPartition = topicAndPartition;
        msgPool = new PartitionMessagePool(talosConfig);
        msgPools.put(getKeyString(topicAndPartition), this);
        LOG.info(String.format("Start processing TopicPartition [%s, %d] with start offset %d",
                topicAndPartition.topicName, topicAndPartition.partitionId, startMessageOffset));
    }

    @Override
    public void process(List<MessageAndOffset> messages, MessageCheckpointer messageCheckpointer) {
        if (msgCheckpointer == null) {
            msgCheckpointer = messageCheckpointer;
        }
        for (MessageAndOffset msg : messages) {
            msgPool.put(new TalosStormMessage(topicPartition, msg));
        }
    }

    @Override
    public void shutdown(MessageCheckpointer messageCheckpointer) {
        msgPools.remove(getKeyString(topicPartition));
        msgPool = null;
        msgCheckpointer = null;
        LOG.info(String.format("Stop processing TopicPartition [%s, %d]",
                topicPartition.topicName, topicPartition.partitionId));
    }
}
