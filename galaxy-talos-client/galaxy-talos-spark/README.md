Talos Spark Streaming
====
----

### 特性

- 易于理解的并行处理：通过使用 DirectTalosInputDStream，Spark Streaming创建的RDD所包含的partitions数量和Talos topic的partitions数量是一一对应的关系，便于理解和使用。
- 升级兼容的容灾机制：通过保存已完成batch的talos offset信息到可靠存储，可以保证作业失败后能够从之前的消费位置恢复，并且避免了spark streaming checkpoint存在的序列化失败隐患（详情可以查看这篇[分享](https://www.mioffice.cn/forum/article_new?id=177))；
- 提供了talos offset和batch级别的监控：通过talos offset lag可以查看当前消费talos的“延迟”情况，通过batch lag可以查看当前的作业积压情况；

### 使用方法

#### 添加依赖
##### Spark 1.6.x用户

```
<dependency>
  <groupId>com.xiaomi.infra.galaxy</groupId>
  <artifactId>galaxy-talos-spark</artifactId>
  <version>${galaxy-talos-spark.version}</version>
</dependency>
<dependency>
  <groupId>com.xiaomi.infra.galaxy</groupId>
  <artifactId>galaxy-talos-sdk</artifactId>
  <version>${galaxy-talos-sdk.version}</version>
</dependency>
```

##### Spark 2.x.y用户

```
<dependency>
  <groupId>com.xiaomi.infra.galaxy</groupId>
  <artifactId>galaxy-talos-spark_2.11</artifactId>
  <version>${galaxy-talos-spark.version}</version>
</dependency>
<dependency>
  <groupId>com.xiaomi.infra.galaxy</groupId>
  <artifactId>galaxy-talos-sdk</artifactId>
  <version>${galaxy-talos-sdk.version}</version>
</dependency>
```

${galaxy-talos-spark.version}和${galaxy-talos-sdk.version}请在[nexus](http://nexus.d.xiaomi.net/)查找当前最新的版本；


#### 常用API

```
import org.apache.spark.streaming.talos._

val directTalosStream: InputDStream[Array[Byte]] = TalosUtils.createDirectStream( streamingContext, [map of Talos parameters], [Talos credential], [set of topics to consume], mo => mo.message.getMessage)
```

1. 如果需要获取更多的talos信息（比如message key/offset），请查看接口说明，自定义参数messageHandler: MessageAndOffset => T
2. 对于[map of Talos parameters]，***必须通过TalosClientConfigKeys.GALAXY_TALOS_SERVICE_ENDPOINT指定Talos[集群地址](http://docs.api.xiaomi.net/talos/inner-cluster-info.html)**。
3. 默认情况下，directTalosStream会从每个Talos topic partition的最新（largest）offset开始消费。如果想要从最旧（smallest）offset开始消费，需要在[map of Talos parameters]中设置auto.offset.reset为smallest。

具体使用方式可以见下面的示例代码。

#### Offset Checkpoint
Offset Checkpoint功能是指在每个batch处理完成之后，自动把该batch的offset信息保存到HDFS中，从而在下次重启的时候，从保存的offset checkpoint文件中自动恢复（类似于talos高阶API的自动commit offset）；
为了方便描述，后续用“原生checkpoint”指代spark streaming自带的checkpoint功能，用“offset checkpoint”指代我们提供的offset保存/恢复功能；
通过使用offset checkpoint，用户可以关闭原生checkpoint功能（原生checkpoint功能在修改代码后容易出现序列化问题）；不过需要注意的是，如果业务逻辑依赖于原生checkpoint（比如使用了mapWithState算子），请勿关闭checkpoint；

##### Offset Checkpoint 使用方式

- 对于新作业，如何开启offset checkpoint功能？
可以在[map of Talos parameters]配置“offset.checkpoint.dir"，示例如下：

```
   val talosStream = TalosUtils.createDirectStream(
        streamingContext,
        Map(TalosClientConfigKeys.GALAXY_TALOS_SERVICE_ENDPOINT -> "http://staging-cnbj2-talos.api.xiaomi.net",
          "auto.offset.reset" -> "smallest",
          "offset.checkpoint.dir" -> "/user/u_wangjiasheng/offset_checkpoint/app_name"),
        talosCredential,
        "talos-topic-name",
         mo => mo.message.getMessage
      )
```
说明：如上，通过配置```"offset.checkpoint.dir" -> "/user/u_wangjiasheng/offset_checkpoint/app_name"```，spark streaming作业在启动的时候就会尝试去指定的目录加载offset消费记录，从而实现从上次离开的位置继续消费；同时，作业运行过程中，会在batch处理结束后把offset信息自动checkpoint到指定的目录；

- 对于已经开启了原生checkpoint功能的作业，如何迁移；

请在StreamingContext启动前，设置如下的system property：
```
 System.setProperty("spark.streaming.talos.offset.checkpoint.dir", "/user/u_wangjiasheng/offset_checkpoint/app_name")
 ```
解释：由于开启原生checkpoint之后，再去修改TalosDStream的配置信息是无法生效的（因为重启作业的时候，TalosDStream会从checkpoint文件中恢复配置信息，而不是加载新配置）；但是加上如上的system property之后，TalosDStream会加载如上配置信息；
加上如上代码后，保持原生checkpoint开启，然后重启作业，则offset checkpoint功能会正常运行；后续重启就可以关闭原生checkpoint了；

#### 示例代码

```
import org.apache.spark.SparkConf
import org.apache.spark.streaming.talos.TalosUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import com.xiaomi.infra.galaxy.rpc.thrift.{Credential, UserType}
import com.xiaomi.infra.galaxy.talos.client.TalosClientConfigKeys

object TalosSparkDemo extends App {
  val batchSec = 5 // spark streaming batch interval;
  val appName = "SparkTalosTest" // spark streaming作业名称;
  val topic = "spark-talos-test" // 需要消费的talos topic名称；
  val talosServiceEndpoint = "http://staging-cnbj2-talos.api.xiaomi.net" //集群信息在这里查询：http://docs.api.xiaomi.net/talos/inner-cluster-info.html
  val keyId = "AKxxxx" //请在融合云"团队管理"服务中查询，密钥管理中的"ID"
  val keySecret = "xxxx" // 密钥管理中的"密钥内容（ SecretKey ）"

  val sparkConf = new SparkConf().setAppName(appName)
  val credential = new Credential()
    .setSecretKeyId(keyId).setSecretKey(keySecret).setType(UserType.DEV_XIAOMI)
  val ssc = new StreamingContext(sparkConf, Seconds(batchSec))

  // 对于之前已经开启了spark streaming checkpoint功能的作业，需要添加如下system property，才能使offset checkpoint生效；
  System.setProperty("spark.streaming.talos.offset.checkpoint.dir", "/user/u_wangjiasheng/offset_checkpoint/app_name")

  val talosDStream = TalosUtils.createDirectStream(
    ssc,
    Map(TalosClientConfigKeys.GALAXY_TALOS_SERVICE_ENDPOINT -> talosServiceEndpoint,
      "auto.offset.reset" -> "smallest",
      // 如下配置可以开启offset checkpoint，类似于talos高阶consumer的自动commit offset；
      "offset.checkpoint.dir" -> "/user/u_wangjiasheng/offset_checkpoint/app_name"),
    credential,
    Set(topic),
    mo => mo.message.getMessage)

  talosDStream.foreachRDD { rdd =>
    rdd.foreachPartition { partition =>
      partition.foreach { bytes =>
        val messageStr = new String(bytes, "UTF-8")
        println(messageStr)
      }
    }
  }

  ssc.start()
  ssc.awaitTermination()
}
```

### 高阶用法


#### 自定义DStream中的数据格式

TalosUtils提供如下API

```
import org.apache.spark.streaming.talos._

val streamingContext = TalosUtils.createDirectStream( [StreamingContext], [map of Talos parameters], [Talos credential], [set of topics to consume], [custom message handler])
```

其中，[custom message handler]是类型为MessageAndOffset => T的参数，T为泛型参数。
比如，想要在DStream中的数据里添加message offset信息的话，可以自定义如下message handler:

```
val messageHandler = (mo: MessageAndOffset) => (mo.message.partitionKey, mo.messageOffset, new String(mo.message.message.array(), Charset.forName("UTF-8")))
```
对于message.array(), 除了转化为String, 也可以转化为其他自定义的内容（**比如可以反序列化为Thrift对象**）, 或者直接返回array.


#### 更多配置参数

以下配置都可以在调用TalosUtils.createDirectStream时，设置在中*streamingContext*的*sparkConf*中的，设置在[map of Talos parameters]中是无效的。

config | default | meaning
----|------|----
spark.streaming.talos.maxRatePerPartition | 0 | 每个partition消费数据的速率限制，message/seconds；如果每个batch数据量太大，导致spark job delay严重时，可以考虑限制每个batch的数据量。
spark.streaming.talos.maxRetries | -1 |  与Talos交互时，失败重试次数；网络不好时，可尝试配置多一些重试次数。默认-1为无限重试。
spark.streaming.talos.backoff.ms | 200 | 与Talos交互时，失败重试次数之间的时间间隔。
spark.streaming.talos.checkpointing.enable | true | 是否自动开启checkpointing,并设置目录为AppName;默认为true,会自动执行streamingContext.checkpoint(AppName);

### Offset Lag监控
Offset lag监控是指当前*消费的offset*和talos topic*最新的offset*的差距，它反映了spark streaming作业处理能力的快慢；如果offset lag增大，则说明spark streaming处理消息太慢了，需要调优或者增加资源；

| Endpoint | Counters |
| ------ | ------ |
| streaming.monitor | TalosOffsetLag/appName={SPARK-APP-NAME},cluster={SPARK-CLUSTER},partition=all,topic={TALOS-TOPIC-NAME},type=srv,user={KERBEROS-PRINCIPAL} |

点击查看[示例](http://falcon.srv/#/chart?id=29184997&graph_type=h&cf=AVERAGE&start=-3600)；
如上"partition=all"是指所有partition的TalosOffsetLag之和；可以通过制定partition=xx来查看某个partition的信息；

### Batch积压监控
除了通过offset lag查看作业积压情况，还可以通过spark streaming的batch积压情况来查看；并且不论topic中的数据流量大小，batch积压监控更方便配置报警（比如积压10个batch后触发报警）；

| Endpoint | Counters |
| ------ | ------ |
| streaming.monitor | 	TalosRDDLag/appName={SPARK-APP-NAME},cluster={SPARK-CLUSTER},topics={TALOS-TOPIC-NAME},type=srv,user={KERBEROS-PRINCIPAL} |

点击查看[示例](http://falcon.srv/#/chart?id=45249121&graph_type=h&cf=AVERAGE&start=-3600)；
说明：此处Counters名为TalosRDDLag，是因为batch积压的情况下，作业中TalosRDD会堆积，因此可以通过这一指标反应积压情况；