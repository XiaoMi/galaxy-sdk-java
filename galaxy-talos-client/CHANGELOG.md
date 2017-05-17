
## 2.1-SNAPSHOT

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

