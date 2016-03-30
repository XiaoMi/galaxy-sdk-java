# 特性

- 易于理解的并行处理：通过使用 DirectTalosInputDStream，Spark Streaming创建的RDD所包含的partitions数量和Talos topic的partitions数量是一一对应的关系，便于理解和使用。
- Exactly-once语义：Offsets可以通过checkpoint保存到可靠存储。因此，通过开启checkpoint（[How to configure Checkpointing](http://spark.apache.org/docs/1.5.2/streaming-programming-guide.html#how-to-configure-checkpointing)），以及使用正确地方式输出到外部存储（[ Semantics of output operations](http://spark.apache.org/docs/1.5.2/streaming-programming-guide.html#semantics-of-output-operations)），可以保证Exactly-once的语义。

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

其中，spark.version需要和spark集群保持一致，galaxy.version一般选择最新即可。

## 代码

**Scala**

```
import org.apache.spark.streaming.talos._

val directTalosStream = TalosUtils.createDirectStream( streamingContext, [map of Talos parameters], [Talos credential], [set of topics to consume])
```

**Java**

```
import org.apache.spark.streaming.talos._

JavaPairInputDStream<String, String> directTalosStream = TalosUtils.createDirectStream( streamingContext, [map of Talos parameters], [Talos credential], [set of topics to consume])
```

得到的directTalosStream类型为：
- Scala: InputDStream[(String, String)]
- Java: JavaPairInputDStream<String, String>

参数(String, String)分别对应Talos消费数据中的(Key, Message)。
对于[map of Talos parameters]，必须通过TalosClientConfigKeys.GALAXY_TALOS_SECURE_SERVICE_ENDPOINT指定Talos集群地址。
默认情况下，directTalosStream会从每个Talos topic partition的最新（largest）offset开始消费。如果想要从最旧（smallest）offset开始消费，需要在[map of Talos parameters]中设置auto.offset.reset为smallest。

**Example**

```
import com.xiaomi.infra.galaxy.rpc.thrift.{Credential, UserType}
import com.xiaomi.infra.galaxy.talos.client.TalosClientConfigKeys
import org.apache.spark.SparkConf
import org.apache.spark.streaming.talos.TalosUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**  * Created by jiasheng on 16-3-28.  */

object TalosTest extends App{  
  val batchSec = 6 
  val appName = "SparkTalosTest" 
  val topic = "spark-talos-test" 
  val uri = "https://***" 
  val key = "***" 
  val secret = "***"

  val sparkConf = new SparkConf().setAppName(appName)
  val ssc = new StreamingContext(sparkConf, Seconds(6))
  val credential = new Credential().setSecretKeyId(key).setSecretKey(secret).setType(UserType.DEV_XIAOMI)

  val talosStream = TalosUtils.createDirectStream(
    ssc,
    Map(TalosClientConfigKeys.GALAXY_TALOS_SECURE_SERVICE_ENDPOINT -> uri,
     "auto.offset.reset" -> "smallest"), 
    credential,
    Set(topic)
  ) 
  talosStream.foreachRDD { rdd =>    
    // rdd operation  
  }  
  ssc.start()  
  ssc.awaitTermination()
}
```

## 高阶

以下特性以Scala示例代码作介绍，Java类同。

### 自定义Stream中的数据

TalosUtils提供如下API

```
import org.apache.spark.streaming.talos._

val directTalosStream = TalosUtils.createDirectStream( streamingContext, [map of Talos parameters], [Talos credential], [set of topics to consume], [custom message handler])
```

其中，[custom message handler]的类型是MessageAndOffset => T，T为泛型参数。
比如，想要在Stream中的数据里添加message offset信息的话，可以自定义如下message handler

```
val messageHandler = (mo: MessageAndOffset) => (mo.message.partitionKey, mo.messageOffset, new String(mo.message.message.array(), Charset.forName("UTF-8")))
```

### 获取每个batch的offset range信息

```
// Hold a reference to the current offset ranges, so it can be used downstream
var offsetRanges = Array[OffsetRange]()
directTalosStream.transform{ rdd =>  
  offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges  
  rdd
}
```

注意两点
- 为了获取batch的offset range信息，必须在directTalosStream初始化后，上面的transform作为第一个方法进行调用，而不是在调用了其他方法后再调用。
- 如果调用了任何触发shuffle或repartition的方法，比如reduceByKey()，RDD partition和Talos partition的一一对应关系不再有效。

### 其他配置

以下配置都可以在调用TalosUtils.createDirectStream时，设置在中*streamingContext*的*sparkConf*中的，设置在[map of Talos parameters]中是无效的。

config | default | meaning 
----|------|---- 
spark.streaming.talos.maxRatePerPartition | 0 | 每个partition消费数据的速率限制，message/seconds；如果每个batch数据量太大，导致spark job delay严重时，可以考虑限制每个batch的数据量。
spark.streaming.talos.maxRetries | 1 |  获取topic offset信息时，失败重试次数；网络不好时，可尝试配置多一些重试次数。
spark.streaming.talos.backoff.ms | 200 | 获取topic offset信息时，失败重试次数之间的时间间隔。

# 反馈
有使用问题，请联系 wangjiasheng@xiaomi.com 。
