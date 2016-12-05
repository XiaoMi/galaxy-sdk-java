# 特性

- 易于理解的并行处理：通过使用 DirectTalosInputDStream，Spark Streaming创建的RDD所包含的partitions数量和Talos topic的partitions数量是一一对应的关系，便于理解和使用。
- Exactly-once语义：Offsets可以通过checkpoint保存到可靠存储。因此，通过开启checkpoint（[How to configure Checkpointing](http://spark.apache.org/docs/1.5.2/streaming-programming-guide.html#how-to-configure-checkpointing)），以及使用正确地方式输出到外部存储（[ Semantics of output operations](http://spark.apache.org/docs/1.5.2/streaming-programming-guide.html#semantics-of-output-operations)），可以保证Exactly-once的语义(默认已经开启checkpointing)。

# 使用方法

## 添加依赖

```
<dependency>
  <groupId>org.apache.spark</groupId>
  <artifactId>spark-streaming_2.10</artifactId>
  <version>${spark.version}</version>
  <scope>provided</scope>
</dependency>
<dependency>
  <groupId>com.xiaomi.infra.galaxy</groupId>
  <artifactId>galaxy-talos-spark</artifactId>
  <version>${galaxy.version}</version>
</dependency>
```

其中，spark.version需要和spark集群保持一致，galaxy.version一般选择最新即可; 
spark-streaming的scope设置与集群情况相关, 如果集群上包含了streaming依赖, 则设置为provided即可; 否则设置为compile.

## 常用API

```
import org.apache.spark.streaming.talos._

val streamingContext = TalosUtils.createStreamingContext( [spark conf], [batch duration], [map of Talos parameters], [Talos credential], [set of topics to consume]){[dstream function]}
```
参数说明:

- [spark conf] 和 [batch duration] 是用来初始化StreamingContext的;
- 对于[map of Talos parameters]，必须通过TalosClientConfigKeys.GALAXY_TALOS_SERVICE_ENDPOINT(作为key)指定Talos集群地址(作为value); 默认情况下，生成的DStream会从每个Talos topic partition的最新（largest）offset开始消费。如果想要从最旧（smallest）offset开始消费，需要在[map of Talos parameters]中设置auto.offset.reset为smallest。
- [Talos credential] 和 [ set of topics to consume] 都是Talos相关概念;
- [dstream function] 是一个function参数, 需要用户传入一个类型为 InputDStream[(String, String)] => Unit 的function来构建DStream; 其中InputDStream[(String, String)] 的(String, String)分别对应Talos消息中的(Key, Message); 如果想要在DStream中处理其他格式的内容, 可以参考下面高阶用法中的[自定义DStream中的数据格式](### 自定义DStream中的数据格式)。

通过上述方式创建的StreamingContext, 默认是开启checkpointing的, 且checkpointing dir是app name; 
这样当重新提交作业的时候, StreamingContext可以从checkpointing数据恢复, 从而可以从上次作业退出的位置(即保存的talos offset信息)继续消费;
如果想要禁止自动开启checkpointing, 可以配置SparkConf *spark.streaming.talos.checkpointing.enable* 为false.


## 示例代码

请查看*galaxy-sdk-java/galaxy-talos-client/galaxy-talos-spark/src/main/scala/com/xiaomi/infra/galaxy/talos/spark/example*

## 高阶用法


### 自定义DStream中的数据格式

TalosUtils提供如下API

```
import org.apache.spark.streaming.talos._

val streamingContext = TalosUtils.createStreamingContext( [spark conf], [batch duration], [map of Talos parameters], [Talos credential], [set of topics to consume], [custom message handler]){[dstream function]}
```

其中，[custom message handler]是类型为MessageAndOffset => T的参数，T为泛型参数。
比如，想要在DStream中的数据里添加message offset信息的话，可以自定义如下message handler:

```
val messageHandler = (mo: MessageAndOffset) => (mo.message.partitionKey, mo.messageOffset, new String(mo.message.message.array(), Charset.forName("UTF-8")))
```
对于message.array(), 除了转化为String, 也可以转化为其他自定义的内容, 或者直接返回array.

### 获取每个batch的offset range信息
以示例代码TalosSparkDemo为参照, 通过一下方式可以获取offset 信息:

```

  // Hold a reference to the current offset ranges, so it can be used downstream
  var offsetRanges = Array[OffsetRange]()

  val ssc = TalosUtils.createStreamingContext(
    sparkConf,
    Seconds(batchSec),
    Map(TalosClientConfigKeys.GALAXY_TALOS_SERVICE_ENDPOINT -> uri,
      "auto.offset.reset" -> "smallest"),
    credential,
    Set(topic)) { inputDstream => {
    inputDstream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }.foreachRDD(rdd => {
      // scalastyle:off println
      rdd.foreach(println)
      // scalastyle:on println
    })
  }

```

注意两点
- 为了获取batch的offset range信息, 上面的transform必须作为inputDStream第一个方法进行调用，而不是在调用了其他方法后再调用。
- 如果调用了任何触发shuffle或repartition的方法，比如reduceByKey()，RDD partition和Talos partition的一一对应关系不再有效。

### 更多配置参数

以下配置都可以在调用TalosUtils.createDirectStream时，设置在中*streamingContext*的*sparkConf*中的，设置在[map of Talos parameters]中是无效的。

config | default | meaning 
----|------|---- 
spark.streaming.talos.maxRatePerPartition | 0 | 每个partition消费数据的速率限制，message/seconds；如果每个batch数据量太大，导致spark job delay严重时，可以考虑限制每个batch的数据量。
spark.streaming.talos.maxRetries | -1 |  与Talos交互时，失败重试次数；网络不好时，可尝试配置多一些重试次数。默认-1为无限重试。
spark.streaming.talos.backoff.ms | 200 | 与Talos交互时，失败重试次数之间的时间间隔。
spark.streaming.talos.checkpointing.enable | true | 是否自动开启checkpointing,并设置目录为AppName;默认为true,会自动执行streamingContext.checkpoint(AppName);

# 反馈
有使用问题，请联系 wangjiasheng@xiaomi.com 。
