package com.xiaomi.infra.galaxy.talos.storm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import com.google.common.base.Optional;
import com.xiaomi.infra.galaxy.talos.client.SimpleTopicAbnormalCallback;
import com.xiaomi.infra.galaxy.talos.client.TalosClientConfigKeys;
import com.xiaomi.infra.galaxy.talos.consumer.*;
import com.xiaomi.infra.galaxy.talos.storm.config.TalosStormConfig;
import com.xiaomi.infra.galaxy.talos.storm.config.TalosStormConfigKeys;
import com.xiaomi.infra.galaxy.talos.storm.message.PartitionMessagePool;
import com.xiaomi.infra.galaxy.talos.storm.message.TalosStormMessage;
import com.xiaomi.infra.galaxy.talos.storm.message.TalosStormMessageProcessor;
import com.xiaomi.infra.galaxy.talos.storm.talos.TalosCluster;
import com.xiaomi.infra.galaxy.talos.thrift.TopicAndPartition;
import com.xiaomi.infra.galaxy.talos.thrift.TopicTalosResourceName;
import libthrift091.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by jiasheng on 16-5-12.
 */
public class TalosStormSpout extends BaseRichSpout {
    private static final Logger LOG = LoggerFactory.getLogger(TalosStormSpout.class);
    private TalosStormConfig talosConfig;
    private SpoutOutputCollector collector;
    private ConcurrentHashMap<String, TalosStormMessageProcessor> msgProcessors;
    private TalosConsumer talosConsumer;
    private int maxRetries;
    private long idleMs;
    private long commitIntervalMs;
    private long lastCommitTime;

    public TalosStormSpout(TalosStormConfig conf) {
        this.talosConfig = conf;
    }

    private void disableAutoCommitOffset() {
        String autoCommit = talosConfig.parameters.get(TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_CHECKPOINT_MESSAGE_OFFSET);
        if (autoCommit != null && autoCommit.equalsIgnoreCase("true")) {
            LOG.warn(String.format("%s should be set to false in TalosStormSpout, " +
                            "otherwise we will manually set it to false to turn off auto offset commit in Talos",
                    TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_CHECKPOINT_MESSAGE_OFFSET));
        }
        talosConfig.parameters.put(TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_CHECKPOINT_MESSAGE_OFFSET, "false");
    }

    private void loadTalosStormParams(Properties properties) {
        idleMs = Integer.valueOf(properties.getProperty(TalosStormConfigKeys.SPOUT_IDLE_MS,
                TalosStormConfigKeys.SPOUT_IDLE_MS_DEFAULT));
        commitIntervalMs = Long.valueOf(properties.getProperty(TalosStormConfigKeys.COMMIT_INTERVAL_MS,
                TalosStormConfigKeys.COMMIT_INTERVAL_MS_DEFAULT));
        if (commitIntervalMs < Long.valueOf(TalosStormConfigKeys.COMMIT_INTERVAL_MS_MIN)) {
            LOG.warn(String.format("Configured %s=%dms is smaller than minimum interval allowed(%sms). " +
                            "Setting value to %sms", TalosStormConfigKeys.COMMIT_INTERVAL_MS, commitIntervalMs,
                    TalosStormConfigKeys.COMMIT_INTERVAL_MS_MIN, TalosStormConfigKeys.COMMIT_INTERVAL_MS_MIN));
            commitIntervalMs = 1000;
        }
        maxRetries = Integer.valueOf(properties.getProperty(TalosStormConfigKeys.MAX_RETRIES,
                TalosStormConfigKeys.MAX_RETRIES_DEFAULT));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        disableAutoCommitOffset();
        Properties talosStormProps = new Properties();
        Properties talosConsumerProps = new Properties();
        for (Map.Entry<String, String> entry : talosConfig.parameters.entrySet()) {
            if (entry.getKey().startsWith(TalosStormConfigKeys.PREFIX)) {
                talosStormProps.setProperty(entry.getKey(), entry.getValue());
            } else {
                talosConsumerProps.setProperty(entry.getKey(), entry.getValue());
            }
        }
        loadTalosStormParams(talosStormProps);
        collector = spoutOutputCollector;
        msgProcessors = new ConcurrentHashMap<String, TalosStormMessageProcessor>();

        TalosConsumerConfig talosConsumerConfig = new TalosConsumerConfig(talosConsumerProps);
        TalosCluster talosCluster = new TalosCluster(talosConsumerConfig, talosConfig.credential);
        while (maxRetries > 0) {
            try {
                TopicTalosResourceName topicResourceName = talosCluster.describeTopic(talosConfig.topic)
                        .topicInfo.topicTalosResourceName;
                talosConsumer = new TalosConsumer(talosConfig.consumerGroupName, talosConsumerConfig,
                        talosConfig.credential, topicResourceName, new MessageProcessorFactory() {
                    @Override
                    public MessageProcessor createProcessor() {
                        return new TalosStormMessageProcessor(talosConfig, msgProcessors);
                    }
                }, new SimpleTopicAbnormalCallback());
                break;
            } catch (TException e) {
                maxRetries--;
                if (maxRetries == 0) {
                    throw new RuntimeException(String.format("Initial TalosStormSpout for topic=%s failed: "
                            + e.getMessage(), talosConfig.topic));
                }
            }
        }
        LOG.info(String.format("Initialized TalosStormSpout for topic=%s groupName=%s with config: %s=%d, %s=%d, %s=%d.",
                talosConfig.topic, talosConfig.consumerGroupName,
                TalosStormConfigKeys.SPOUT_IDLE_MS, idleMs,
                TalosStormConfigKeys.COMMIT_INTERVAL_MS, commitIntervalMs,
                TalosStormConfigKeys.MAX_RETRIES, maxRetries));
    }

    @Override
    public void nextTuple() {
        if (shouldCommit()) {
            commitOffsets();
        }

        boolean emitted = false;
        for (TalosStormMessageProcessor processor : msgProcessors.values()) {
            Optional<PartitionMessagePool> msgPoolOpt = processor.getMessagePool();
            if (msgPoolOpt.isPresent()) {
                Optional<TalosStormMessage> msgOpt = msgPoolOpt.get().get();
                if (msgOpt.isPresent()) {
                    Iterable<List<Object>> tuples = talosConfig.talosScheme
                            .generateTuples(msgOpt.get().topicPartition, msgOpt.get().message);
                    for (List<Object> tuple : tuples) {
                        collector.emit(tuple, msgOpt.get().id);
                    }
                    emitted = true;
                }
            }
        }
        if (!emitted) {
            try {
                LOG.debug(String.format("No data to emmit, TalosStormSpout will sleep %dms",
                        idleMs));
                Thread.sleep(idleMs);
            } catch (InterruptedException e) {
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(talosConfig.talosScheme.getOutputFields());
    }

    @Override
    public void ack(Object msgId) {
        TalosStormMessageProcessor processor = msgProcessors.get(retrieveTopicPartitionFromId(msgId));
        if (processor != null) {
            Optional<PartitionMessagePool> msgPoolOpt = processor.getMessagePool();
            if (msgPoolOpt.isPresent()) {
                msgPoolOpt.get().ack(retrieveOffsetFromId(msgId));
            }
        }
    }

    @Override
    public void fail(Object msgId) {
        TalosStormMessageProcessor processor = msgProcessors.get(retrieveTopicPartitionFromId(msgId));
        if (processor != null) {
            Optional<PartitionMessagePool> msgPoolOpt = processor.getMessagePool();
            if (msgPoolOpt.isPresent()) {
                msgPoolOpt.get().fail(retrieveOffsetFromId(msgId));
            }
        }
    }


    private boolean shouldCommit() {
        if (lastCommitTime == 0) {
            lastCommitTime = System.currentTimeMillis();
            return false;
        }
        return System.currentTimeMillis() - lastCommitTime >= commitIntervalMs;
    }

    private void commitOffsets() {
        for (TalosStormMessageProcessor processor : msgProcessors.values()) {
            Optional<TopicAndPartition> tpOpt = processor.getTopicPartition();
            Optional<PartitionMessagePool> msgPoolOpt = processor.getMessagePool();
            Optional<MessageCheckpointer> checkpointerOpt = processor.getMessageCheckpointer();
            if (tpOpt.isPresent() && msgPoolOpt.isPresent() && checkpointerOpt.isPresent()) {
                Optional<Long> commitOffset = msgPoolOpt.get().getCommitOffset();
                if (commitOffset.isPresent()) {
                    checkpointerOpt.get().checkpoint(commitOffset.get());
                    LOG.debug(String.format("Committed offset for [%s, %d] with value %d.",
                            tpOpt.get().topicName, tpOpt.get().partitionId, commitOffset.get()));
                }
            }
        }
        lastCommitTime = System.currentTimeMillis();
    }

    private String retrieveTopicPartitionFromId(Object id) {
        String idStr = (String) id;
        return idStr.substring(0, idStr.lastIndexOf("-"));
    }

    private long retrieveOffsetFromId(Object id) {
        String idStr = (String) id;
        return Long.valueOf(idStr.substring(idStr.lastIndexOf("-") + 1));
    }


    @Override
    public void close() {
        commitOffsets();
        talosConsumer.shutDown();
        LOG.info(String.format("Close TalosStormSpout for topic = %s with groupName = %s.",
                talosConfig.topic, talosConfig.consumerGroupName));
    }
}
