package com.xiaomi.infra.galaxy.talos.storm.config;

/**
 * Created by jiasheng on 16-5-31.
 */
public class TalosStormConfigKeys {
    public static final String PREFIX = "galaxy.talos.storm.";
    public static final String PARTITION_QUEUE_SIZE = PREFIX + "partition.queue.size";
    public static final String COMMIT_INTERVAL_MS = PREFIX + "commit.interval.ms";
    public static final String MAX_RETRIES = PREFIX + "max.retries";
    public static final String SPOUT_IDLE_MS = PREFIX + "spout.idle.ms";

    public static final String PARTITION_QUEUE_SIZE_DEFAULT = "1000";
    public static final String COMMIT_INTERVAL_MS_DEFAULT = "60000";
    public static final String COMMIT_INTERVAL_MS_MIN = "1000";
    public static final String MAX_RETRIES_DEFAULT = "2";
    public static final String SPOUT_IDLE_MS_DEFAULT = "50";
}
