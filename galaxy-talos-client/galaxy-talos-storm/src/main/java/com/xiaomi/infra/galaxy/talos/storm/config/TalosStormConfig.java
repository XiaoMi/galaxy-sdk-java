package com.xiaomi.infra.galaxy.talos.storm.config;

import com.google.common.collect.ImmutableMap;
import com.xiaomi.infra.galaxy.rpc.thrift.Credential;
import com.xiaomi.infra.galaxy.talos.client.TalosClientConfigKeys;
import com.xiaomi.infra.galaxy.talos.consumer.TalosConsumerConfig;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by jiasheng on 16-5-12.
 */
public class TalosStormConfig implements Serializable {
    public final String topic;
    public final String consumerGroupName;
    public final Credential credential;
    public final Map<String, String> parameters = new HashMap<String, String>();
    public TalosStormScheme talosScheme = new DefaultTalosStormScheme();

    public TalosStormConfig(String topic, String consumerGroupName, Credential credential, String talosEndpoint) {
        this.topic = topic;
        this.consumerGroupName = consumerGroupName;
        this.credential = credential;
        this.parameters.put(TalosClientConfigKeys.GALAXY_TALOS_SERVICE_ENDPOINT, talosEndpoint);
    }
}
