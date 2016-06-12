# POM依赖


```
    <properties>
        <storm.version></storm.version>
        <talos.version></talos.version>
    </properties>
    <dependencies>
        <dependency>
            <groupId>com.xiaomi.infra.galaxy</groupId>
            <artifactId>galaxy-talos-storm</artifactId>
            <version>${talos.version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.storm</groupId>
            <artifactId>storm-core</artifactId>
            <version>${storm.version}</version>
            <scope>provided</scope>
        </dependency>
    </dependencies>
```
storm.version和talos.version根据实际情况设置。

# 初始化Spout

## 配置Config

TalosStormConfig的构造函数非常简单：

```
public TalosStormConfig(String topic, String consumerGroupName, Credential credential, String talosEndpoint)
```
其中，topic是需要读取数据的talos topic名称；consumerGroupName主要和offset信息的保存和读取有关；Credential的使用请参考[Talos Wiki](http://awsbj0.talos.api.xiaomi.com/sdk/sdk-step-by-step.html)；talosEndpoint是Talos集群地址，即TalosConsumer初始化过程中需要配置的*galaxy.talos.service.endpoint*。

### 自定义参数
一些可自定义的参数，可以设置在TalosStormConfig.parameters中。例如：
```
talosStormConfig.parameters.put(TalosStormConfigKeys.COMMIT_INTERVAL_MS, "6000");
```
可配置的参数可以在TalosStormConfigKeys中找到，具体含义和默认配置如下：

| 配置| 说明| 默认值 |
|:-:|:-:|:-:|
|PARTITION_QUEUE_SIZE|缓存从Talos读取且未emit出去的消息的队列大小| 1000 |
| COMMIT_INTERVAL_MS |commit offset的时间间隔| 60000 |
| MAX_RETRIES | 初始化TalosConsumer的失败重试次数 | 2 |
|SPOUT_IDLE_MS| 当没有消息可以emit时，spout会sleep该配置时间后再重试|50|
如果有其他TalosConsumer相关的自定义参数，也可以配置在TalosStormConfig.parameters中，例如：
```
talosStormConfig.parameters.put(TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_FETCH_INTERVAL, 200)
```
### 自定义Scheme
Scheme控制TalosStormSpout emit出去的消息内容。TalosStormConfig默认的Scheme是DefaultTalosStormScheme，生成的tuple一共有四个域，按顺序分别是：
*toipic, offset, topic, partition*
TalosStormSpout下游的Bolt获取tuple中数据的方式如下：
```
String msg = tuple.getStringByField(DefaultTalosStormScheme.MESSAGE_STRING_SCHEME_KEY);
long offset = tuple.getLongByField(DefaultTalosStormScheme.OFFSET_LONG_SCHEME_KEY);
String topic = tuple.getStringByField(DefaultTalosStormScheme.TOPIC_STRING_SCHEME_KEY);
int partition = tuple.getIntegerByField(DefaultTalosStormScheme.PARTITION_INT_SCHEME_KEY);
```
如果需要自定义Scheme，实现TalosStormScheme接口并配置在TalosStormConfig中即可。
主要接口实现示例：
```
    @Override
    public Iterable<List<Object>> generateTuples(TopicAndPartition topicPartition, MessageAndOffset msg) {
        List<Object> tuple = new ArrayList<Object>();
        tuple.add(new String(msg.getMessage().getMessage(), Charsets.UTF_8));
        tuple.add(msg.messageOffset);
        tuple.add(topicPartition.topicName);
        tuple.add(topicPartition.partitionId);
        return Arrays.asList(tuple);
    }

    @Override
    public Fields getOutputFields() {
        return new Fields("message", "offset", "topic", "partition");
    }
```
## 构造TalosStormSpout
TalosStormSpout只有一个构造函数，传入已经配置好的TalosStormConfig即可。Topology的构造和提交，请参考[storm官方wiki](http://storm.apache.org/releases/1.0.1/Running-topologies-on-a-production-cluster.html)。

#示例代码
```
    private final String topic = "talos-storm-topic";
    private final String group = "talos-storm-group";
    private final String keyId = "";
    private final String key = "";
    private final String talosEndpoint = "";

    public StormTopology buildTopology() {
        Credential credential = new Credential()
                .setSecretKeyId(keyId)
                .setSecretKey(key)
                .setType(UserType.DEV_XIAOMI);

        TalosStormConfig config = new TalosStormConfig(topic,
                group, credential, talosEndpoint);
        
        config.parameters.put(TalosStormConfigKeys.COMMIT_INTERVAL_MS, "60000");
        config.parameters.put(TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_FETCH_INTERVAL, "200");
        
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("talos-reader", new TalosStormSpout(config), 4);
        builder.setBolt("word-count", new WordCountBolt(), 1)
                .shuffleGrouping("talos-reader");
        return builder.createTopology();
    }
```

#联系
使用问题及BUG反馈，请联系 *wangjiasheng@xiaomi.com* 。