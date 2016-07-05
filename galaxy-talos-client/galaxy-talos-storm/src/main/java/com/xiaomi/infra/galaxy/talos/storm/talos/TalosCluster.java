package com.xiaomi.infra.galaxy.talos.storm.talos;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.xiaomi.infra.galaxy.rpc.thrift.Credential;
import com.xiaomi.infra.galaxy.talos.admin.TalosAdmin;
import com.xiaomi.infra.galaxy.talos.consumer.SimpleConsumer;
import com.xiaomi.infra.galaxy.talos.consumer.TalosConsumerConfig;
import com.xiaomi.infra.galaxy.talos.thrift.DescribeTopicRequest;
import com.xiaomi.infra.galaxy.talos.thrift.Topic;
import com.xiaomi.infra.galaxy.talos.thrift.TopicAndPartition;
import libthrift091.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provide talos clients to use
 * Created by jiasheng on 16-5-12.
 */
public class TalosCluster {
    private static final Logger LOG = LoggerFactory.getLogger(TalosCluster.class);
    private final TalosConsumerConfig config;
    private final Credential credential;
    private TalosAdmin talosAdmin;
    private LoadingCache<TopicAndPartition, SimpleConsumer> cachedSimpleConsumers;

    public TalosCluster(TalosConsumerConfig talosConsumerConfig, final Credential credential) {
        this.config = talosConsumerConfig;
        this.credential = credential;
        cachedSimpleConsumers = CacheBuilder.newBuilder()
                .build(new CacheLoader<TopicAndPartition, SimpleConsumer>() {
                    @Override
                    public SimpleConsumer load(TopicAndPartition topicAndPartition) {
                        return new SimpleConsumer(config, topicAndPartition, credential);
                    }
                });
    }


    public synchronized TalosAdmin getAdmin() {
        if (talosAdmin == null) {
            talosAdmin = new TalosAdmin(config, credential);
            LOG.info("Initialized TalosAdmin.");
        }
        return talosAdmin;
    }

    public SimpleConsumer getSimpleConsumer(TopicAndPartition topicPartition) {
        return cachedSimpleConsumers.getUnchecked(topicPartition);
    }

    public Topic describeTopic(String topic) throws TException {
        return getAdmin().describeTopic(new DescribeTopicRequest(topic));
    }

}
