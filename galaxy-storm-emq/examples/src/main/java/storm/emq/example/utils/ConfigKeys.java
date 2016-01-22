package storm.emq.example.utils;


public class ConfigKeys {
    // config for storm
    // topology name, must set;
    public static String STORM_TOPOLOGY_NAME = "storm.topology.name";
    // nimbus hostname, can't be ip, must set;
    public static String STORM_NIMBUS_HOSTNAME = "storm.nimbus.hostname";
    // worker number, must set;
    public static String STORM_WORKER_NUMBER = "storm.worker.number";
    // storm cluster mode, local or remote, must set;
    public static String STORM_CLUSTER_MODE = "storm.cluster.mode";

    //config for EMQ
    //EMQ queue name, must set
    public static String EMQ_QUEUE_NAME = "emq.queue.name";
    //EMQ endpoint, optional
    public static String EMQ_QUEUE_ENDPOINT = "emq.queue.endpoint";
    //EMQ tag, optional
    public static String EMQ_QUEUE_TAG = "emq.queue.tag";
    //Credential needed, must set
    public static String SECRET_KEY_ID = "secret.key.id";
    //Credential needed, must set
    public static String SECRET_KEY = "secret.key";
}
