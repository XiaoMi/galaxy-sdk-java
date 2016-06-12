package com.xiaomi.infra.galaxy.talos.storm.message;

import com.google.common.base.Optional;
import com.xiaomi.infra.galaxy.talos.client.TalosClientConfigKeys;
import com.xiaomi.infra.galaxy.talos.storm.config.TalosStormConfig;
import com.xiaomi.infra.galaxy.talos.storm.config.TalosStormConfigKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by jiasheng on 16-5-31.
 */
public class PartitionMessagePool {
    private static final Logger LOG = LoggerFactory.getLogger(PartitionMessagePool.class);
    private BlockingQueue<TalosStormMessage> newMsg;
    private TreeMap<Long, TalosStormMessage> inflightMsg;
    private TreeSet<Long> retryMsg;
    private Long emittedOffset;

    public PartitionMessagePool(TalosStormConfig config) {
        int queueSize;
        if (config.parameters.containsKey(TalosStormConfigKeys.PARTITION_QUEUE_SIZE)) {
            queueSize = Integer.valueOf(config.parameters.get(TalosStormConfigKeys.PARTITION_QUEUE_SIZE));
        } else {
            queueSize = Integer.valueOf(TalosStormConfigKeys.PARTITION_QUEUE_SIZE_DEFAULT);
        }
        newMsg = new LinkedBlockingQueue<TalosStormMessage>(queueSize);
        inflightMsg = new TreeMap<Long, TalosStormMessage>();
        retryMsg = new TreeSet<Long>();
        LOG.info(String.format("Init PartitionMessagePool with %s=%d.",
                TalosStormConfigKeys.PARTITION_QUEUE_SIZE, queueSize));
    }

    public void put(TalosStormMessage message) {
        boolean success = false;
        while (!success) {
            try {
                newMsg.put(message);
                success = true;
            } catch (InterruptedException e) {
                LOG.warn("Failed to put message into queue. Retry immediately.");
            }
        }
    }

    public Optional<TalosStormMessage> get() {
        if (!retryMsg.isEmpty()) {
            Long offset = retryMsg.pollFirst();
            return Optional.of(inflightMsg.get(offset));
        }
        TalosStormMessage msg = newMsg.poll();
        if (msg != null) {
            inflightMsg.put(msg.message.messageOffset, msg);
            emittedOffset = msg.message.messageOffset;
        }
        return Optional.fromNullable(msg);
    }

    public void ack(long offset) {
        inflightMsg.remove(offset);
    }

    public void fail(long offset) {
        retryMsg.add(offset);
    }

    public Optional<Long> getCommitOffset() {
        Map.Entry<Long, TalosStormMessage> firstEntry = inflightMsg.firstEntry();
        if (firstEntry == null) {
            return Optional.fromNullable(emittedOffset);
        } else {
            return Optional.of(firstEntry.getKey() - 1);
        }
    }
}
