## 2.4.0
  * D153930: optimize describeTopic problem by use a new interface
             change some log to debugLog to reduce client log
             fix consumer thread exit problem and simpleConsumerDemo bug
             add getWorkId interface and sdk threadName
  * D156304: add hbase operation blocked exception to thrift

## 2.3.11
  * D150406: add new send message interface for flink-talos

## 2.3.10
  * D150019: change thrift INVALID_TOPIC_NAME to INVALID_NAME_ERROR

## 2.3.9
  * reduce consume interval

## 2.3.8
  * D141385: fix consume but not commit question 

## 2.3.7 
  * D139020: change oraclejdk1.6 to openjdk1.8

## 2.3.6
  * D134066: add queryConsumerOffsetLag function for talos admin 

## 2.3.5
  * D117528: update sdk by quota auto-apply

## 2.3.4
  * D114818: make lockWorker retry work
  * D112899: fix bug: thrift error when GalaxyTalosException
  * D108279: optimize scheduleinfo shutdown interface
  * D108220: Revert "talos sdk add dns timeout"

## 2.3.3
  * D107012: to prevent frequent getScheduleInfo call

## 2.3.2
  * D104305: optimize spark talos producer demo
  * D104122: retry to describe topic in case of throttle exception
  * D101753: Optimize fetching talos messages
  * D101666: Add scala talos producer demo
  * D100865: fix bug for auto location log incorrect

## 2.3.1
  * D99490: edit unit test for talos auto location
  * D99259: talos sdk add dns timeout
  * D99045: add auto location log
  * D98915: Discard checkpointed offsets when topic is recreated
  * D97669: add log for talos auto location

## 2.3.0
  * D85787: add talos auto location feature
  * D95872: edit demos for eco account

## 2.2.1-jdk1.8

## 2.2.1
  * D91454: add error code for throttling

## 2.2.0
  * D84780: add quota operation for sdk

## 2.1.8
  * D83974: Add TalosMiniCluster for talos-spark tests & Check talos partition  OffsetInfo when generating new TalosRDD

## 2.1.7
  * D81286: Calculate offset lag without fetching latest offset

## 2.1.6
  * D78866: update TalosClient add listTopicsinfo
  * D78568: simpleProducer putMessage reject message size larger than 20MB
  * D77343: Fix talos simple consumer id
  * D73606: consumer support read back

## 2.1.5
  * D73159: talos producer add msg timeout & limit msg block size
  * D71982: fix creating eco topic
  * D70313: fix talos client not print exception stack

## 2.1.4
  * D70083: add timeout timestamp for putMessage and getMessage request

## 2.1.3
  * D59608: [TALOS] Fix SparkContext NotSerializableException
  * D66529: Increase backoff time after each retry
  * D69538: Delete offset dir in case of partition number changed

## 2.1.2
  * D58252: Do not serialize custom class
  * D58243: Set unique consumer id to talos SimpleConsumer
  * D58240: [TALOS] set custom consumer id to SimpleConsumer
  * D57195: Save/Restore offsets through HDFS

## 2.1.1
  * D54643: Push number of active rdds to perfcounter
  * D55173: Add offset range debug info
  * D55360: Add SerialVersionUID
  * D55372: Check error code of offset info

## 2.1.0
  * D50402: fix getMessageSize for MessageSerializerV3
  * D49238: add createTimestampList for MessageBlock && use thrift serialize for Message
  * D48163: fromOffset must not be larger than untilOffset
  * D47983: Fix partition offset lag

## 2.0.3
  * D46376: Add ConsumerOffsetLag reporter
  * D46327: update thrift generated files
  * D45248: Catch exception when fetching messages/offsets from talos and retry
  * D45241: UntilOffset must not be smaller than FromOffset 

## 2.0.2
  * D44022: sdk add msg createTime for block & consumer add msg land time

## 2.0.1
  * D43656: add timestamp for Message when Message not set timestamp
  * D43579: add data migrate demo

## 2.0.0
  * D43226: add messageVersion for MessageCompression interface
  * D42885: sdk update for smooth upgrade
  * D42597: add MessageType for talos SerializationV2
  * D42329: add check message sequenceNumber logic
  * D42328: add serialization for talos
  * D41637: sdk support cloud-auth topic name check
  * D41038: fix talos mapreduce bug

## 1.2.1
  * D40435: update demo by user-return-visit
  * D40143: wrap query offset logic & make offset setting flexible for user start/restart consumer
  * D39981: update sdk permission def

## 1.2.0
  * D39621: exclude org.testng from talos sdk
  * D38896: Autoset checkpointing of StreamingContext
  * D39223: Format talos mapreduce config: fetch message with endMessageOffset


## 1.1.4
  * D37901: udpate config init method
  * D35369: Replace 'mdh' dependency with community dependency for spark and storm
  * D35632: fix sdk may put null msg
  * D35648/D35864: fix sdk naming checking
  * D35735/D36159/D36783: add TalosTopicInputFormat for mapreduce job
  * D35961: Change scope of spark-streaming to compile


## 1.1.3
  * D34943/D35154: SDK support SDS Stream Auth


## 1.1.2
  * D33949: Add talos-storm plugin
  * D34377: Make config serializable


## 1.1.1
  * D34190: Fix log info boring to application
  * D33815: Add unhandled msg number for fetched messages
  * D33736: Using Properties instead of hadoop.conf
  * D33707: add shutdown for TalosConsumer
  * D33652: add shutdown for TalosProducer
  * D33380: Change generated files version to thrift 0.9.2
  * D33368: MessageProcessor support checkpoint consumed message offset
  * D33222: Fix simple consumer bug when startOffset==-1/-2


## 1.1.0
  * D32867: Enlarge message bytes limit
  * D32717: Change SimplePartitioner to adjust range partition
  * D32260: Bug fix: SimpleConsumer fetchMessage sometimes return some old data
  * D32045: Update talos sdk pom for independent deploy


## 1.0.0

  * Initial release!

