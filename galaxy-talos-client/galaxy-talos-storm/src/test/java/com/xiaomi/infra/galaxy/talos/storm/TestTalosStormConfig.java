package com.xiaomi.infra.galaxy.talos.storm;

import com.xiaomi.infra.galaxy.talos.storm.config.TalosStormConfig;

/**
 * Created by jiasheng on 16-6-2.
 */
public class TestTalosStormConfig {
    private static TalosStormConfig config;
    public static final String topic = "test-topic";
    public static final String group = "test-group";

    public static synchronized TalosStormConfig getConfig() {
        if (config == null) {
            config = new TalosStormConfig(topic, group, null, null);
        }
        return config;
    }
}
