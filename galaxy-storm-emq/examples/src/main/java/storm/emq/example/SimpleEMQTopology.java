package storm.emq.example;

import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import com.xiaomi.infra.galaxy.rpc.thrift.Credential;
import com.xiaomi.infra.galaxy.rpc.thrift.UserType;
import storm.emq.EMQConfig;
import storm.emq.EMQSpout;
import storm.emq.example.utils.ConfigHelper;
import storm.emq.example.utils.ConfigKeys;
import storm.emq.example.utils.SubmitTopologyHelper;

import java.util.Map;

/**
 * Created by jiasheng on 15-12-29.
 */
public class SimpleEMQTopology {
    private final Map conf;

    public SimpleEMQTopology(Map conf) {
        this.conf = conf;
    }

    private EMQConfig getEMQConfig() {
        String queueName = ConfigHelper.getString(conf, ConfigKeys.EMQ_QUEUE_NAME);
        String endpoint = ConfigHelper.getString(conf, ConfigKeys.EMQ_QUEUE_ENDPOINT);
        String tag = ConfigHelper.getString(conf, ConfigKeys.EMQ_QUEUE_TAG);
        String keyId = ConfigHelper.getLong(conf, ConfigKeys.SECRET_KEY_ID).toString();
        String key = ConfigHelper.getString(conf, ConfigKeys.SECRET_KEY);
        Credential credential = new Credential().setSecretKeyId(keyId).setSecretKey(key).setType(UserType.APP_SECRET);

        return new EMQConfig(endpoint, queueName, tag, credential);
    }

    private StormTopology buildTopology() {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        EMQSpout spout = new EMQSpout(getEMQConfig());
        LogBolt bolt = new LogBolt();

        topologyBuilder.setSpout("spout", spout, 4);
        topologyBuilder.setBolt("bolt", bolt).shuffleGrouping("spout");
        return topologyBuilder.createTopology();
    }

    public static void main(String[] args) throws Exception {

        if (args == null || args.length != 1) {
            System.err.println("Usage: ./bin/storm jar "
                    + "${your_topology-jar-with-dependencies.jar}"
                    + "${package.path.main.class} ${config_file_for_the_topology}");
            System.exit(-1);
        }

        // setup StormTopology
        String topologyConfigFile = args[0];
        Map topologyConfig = ConfigHelper.getTopologyConfig(topologyConfigFile);
        SimpleEMQTopology topology = new SimpleEMQTopology(topologyConfig);
        StormTopology stormTopology = topology.buildTopology();

        SubmitTopologyHelper.submitTopology(stormTopology, topologyConfig);
    }
}
