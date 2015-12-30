package storm.emq.example.utils;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;

import java.util.Map;

/**
 * Created by jiasheng on 15-9-17.
 */
public class SubmitTopologyHelper {
    public static void submitTopology(StormTopology stormTopology, Map topologyConfig) throws Exception {
        // setup StormTopology

        Config submitConfig = new Config();

        // set the configuration for topology
        submitConfig.put(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS, 5000);
        submitConfig.put(Config.TOPOLOGY_ACKER_EXECUTORS, 100);
        submitConfig.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 20);

        // set the worker process number
        submitConfig.setNumWorkers(ConfigHelper.getInt(topologyConfig, ConfigKeys.STORM_WORKER_NUMBER));

        // get topologyName adn clusterMode;
        String topologyName = ConfigHelper.getString(topologyConfig, ConfigKeys.STORM_TOPOLOGY_NAME);
        String clusterMode = ConfigHelper.getString(topologyConfig, ConfigKeys.STORM_CLUSTER_MODE);

        if (clusterMode.equals("local")) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("storm-emq", submitConfig, stormTopology);
        } else {
            submitConfig.put(Config.NIMBUS_HOST, ConfigHelper.getString(topologyConfig, ConfigKeys.STORM_NIMBUS_HOSTNAME));
            StormSubmitter.submitTopology(topologyName, submitConfig, stormTopology);
        }

    }
}
