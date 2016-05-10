package org.apache.spark.streaming.talos

import com.xiaomi.infra.galaxy.rpc.thrift.Credential
import com.xiaomi.infra.galaxy.talos.admin.TalosAdmin
import com.xiaomi.infra.galaxy.talos.client.TalosClientConfig
import com.xiaomi.infra.galaxy.talos.consumer.{SimpleConsumer, TalosConsumerConfig}
import com.xiaomi.infra.galaxy.talos.thrift.{DescribeTopicRequest, GetTopicOffsetRequest, TopicAndPartition, TopicTalosResourceName}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkException
import org.apache.spark.streaming.talos.TalosCluster.Offset.Offset
import org.apache.spark.streaming.talos.TalosCluster.{Err, Offset}

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable

/**
  * Created by jiasheng on 16-3-15.
  */
private[spark]
class TalosCluster(
  val talosParams: Map[String, String],
  val credential: Credential
) extends Serializable {
  @transient
  private var _config: Configuration = null
  @transient
  private var _talosAdmin: TalosAdmin = null
  @transient
  private var _cacheSimpleConsumer: mutable.Map[TopicPartition, SimpleConsumer] = null
  @transient
  private var _topicResourceNames: mutable.Map[String, TopicTalosResourceName] = null

  def config: Configuration = this.synchronized {
    if (_config == null) {
      _config = new Configuration()
      talosParams.foreach { case (k, v) =>
        _config.set(k, v)
      }
    }
    _config
  }

  def admin(): TalosAdmin = this.synchronized {
    if (_talosAdmin == null) {
      _talosAdmin = new TalosAdmin(new TalosClientConfig(config), credential)
    }
    _talosAdmin
  }

  def topicResourceName(topic: String): TopicTalosResourceName = this.synchronized {
    if (_topicResourceNames == null) {
      _topicResourceNames = mutable.Map.empty[String, TopicTalosResourceName]
    }
    _topicResourceNames.getOrElseUpdate(topic,
      admin().describeTopic(new DescribeTopicRequest(topic)).topicInfo.topicTalosResourceName)
  }

  def simpleConsumer(topic: String, partition: Int): SimpleConsumer = this.synchronized {
    if (_cacheSimpleConsumer == null) {
      _cacheSimpleConsumer = mutable.Map.empty[TopicPartition, SimpleConsumer]
    }
    val topicPartition = new TopicPartition((topic, partition))
    _cacheSimpleConsumer.getOrElseUpdate(topicPartition, new SimpleConsumer(
      new TalosConsumerConfig(config),
      new TopicAndPartition(topic, topicResourceName(topic), partition),
      credential)
    )
  }

  def getLatestOffsets(topics: Set[String]): Either[Err, Map[TopicPartition, Long]] = {
    getOffsets(topics, Offset.Latest)
  }

  def getEarliestOffsets(topics: Set[String]): Either[Err, Map[TopicPartition, Long]] = {
    getOffsets(topics, Offset.Earliest)
  }


  private def getOffsets(
    topics: Set[String],
    offset: Offset
  ): Either[Err, Map[TopicPartition, Long]] = {
    try {
      val result = topics.map(topic => {
        val partitionOffsets = admin().getTopicOffset(
          new GetTopicOffsetRequest(topicResourceName(topic)))

        import scala.collection.JavaConverters._
        partitionOffsets.asScala.map(po =>
          new TopicPartition((topic, po.partitionId)) ->
            (offset match {
              case Offset.Earliest => po.startOffset
              // in Kafka, latest offset is LogEndOffset:
              // The offset of the next message that WILL be appended to the log
              case Offset.Latest => po.endOffset + 1
            })
        ).toMap
      }).fold(Map[TopicPartition, Long]())((l, r) => l ++ r)

      Right(result)
    } catch {
      case e: Exception =>
        val err = new Err
        err.append(e)
        Left(err)
    }
  }

}

private[spark]
object TalosCluster {
  type Err = ArrayBuffer[Throwable]

  object Offset extends Enumeration {
    type Offset = Value
    val Earliest, Latest = Value
  }

  /** If the result is right, return it, otherwise throw SparkException */
  def checkErrors[T](result: Either[Err, T]): T = {
    result.fold(
      errs => throw new SparkException(errs.mkString("\n")),
      ok => ok
    )
  }
}
