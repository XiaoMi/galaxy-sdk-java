package org.apache.spark.streaming.talos

import java.util.Properties

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{Logging, SparkException}

import com.xiaomi.infra.galaxy.rpc.thrift.Credential
import com.xiaomi.infra.galaxy.talos.admin.TalosAdmin
import com.xiaomi.infra.galaxy.talos.client.TalosClientConfig
import com.xiaomi.infra.galaxy.talos.consumer.{SimpleConsumer, TalosConsumerConfig}
import com.xiaomi.infra.galaxy.talos.thrift._

/**
 * Created by jiasheng on 16-3-15.
 */
@SerialVersionUID(-995673360675374997L)
private[spark]
class TalosCluster(
    val talosParams: Map[String, String],
    val credential: Credential
) extends Serializable with Logging {

  import TalosCluster._

  @transient
  private[talos] var _config: Properties = null
  @transient
  private[talos] var _talosAdmin: TalosAdmin = null

  def config: Properties = this.synchronized {
    if (_config == null) {
      _config = new Properties()
      talosParams.foreach { case (k, v) =>
        _config.setProperty(k, v)
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
    this.simpleConsumer(topic, partition, Option.empty[String])
  }

  def simpleConsumer(
      topic: String,
      partition: Int,
      simpleConsumerIdOpt: Option[String]
  ): SimpleConsumer = this.synchronized {
    if (_cacheSimpleConsumer == null) {
      _cacheSimpleConsumer = mutable.Map.empty[TopicPartition, SimpleConsumer]
    }

    val topicPartition = new TopicPartition(topic, partition)

    def getConsumer(): SimpleConsumer = {
      val consumer = new SimpleConsumer(
        new TalosConsumerConfig(config),
        new TopicAndPartition(topic, topicResourceName(topic), partition),
        credential)
      simpleConsumerIdOpt.foreach(consumer.setSimpleConsumerId)
      consumer
    }

    _cacheSimpleConsumer.getOrElseUpdate(topicPartition, getConsumer)
  }

  def getLatestOffsets(topics: Set[String]): Either[Err, Map[TopicPartition, Long]] = {
    getOffsets(topics).right.map(offsetMap => offsetMap.map {
      case (topicPartition, (_, untilOffset)) => (topicPartition, untilOffset)
    })
  }

  def getEarliestOffsets(topics: Set[String]): Either[Err, Map[TopicPartition, Long]] = {
    getOffsets(topics).right.map(offsetMap => offsetMap.map {
      case (topicPartition, (fromOffset, _)) => (topicPartition, fromOffset)
    })
  }

  // topic_partition -> (from_offset, until_offset);
  def getOffsets(topics: Set[String]): Either[Err, Map[TopicPartition, (Long, Long)]] = {
    try {
      val result = topics.map(topic => {
        val partitionOffsets = admin().getTopicOffset(
          new GetTopicOffsetRequest(topicResourceName(topic)))

        import scala.collection.JavaConverters._
        partitionOffsets.asScala.map(po => {
          if (po.errorCode != null) {
            val exception = new GalaxyTalosException()
            exception.setErrorCode(po.errorCode)
            exception.setErrMsg(po.errorMsg)
            throw exception
          }

          assert(po.startOffset != -1, s"Invalid OffsetInfo: $po")

          // in Kafka, latest offset is LogEndOffset:
          // The offset of the next message that WILL be appended to the log
          new TopicPartition((topic, po.partitionId)) -> (po.startOffset, po.endOffset + 1)
        }
        ).toMap
      }).fold(Map[TopicPartition, (Long, Long)]())((l, r) => l ++ r)
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

  @transient
  private[talos] var _cacheSimpleConsumer: mutable.Map[TopicPartition, SimpleConsumer] = null
  @transient
  private[talos] var _topicResourceNames: mutable.Map[String, TopicTalosResourceName] = null

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
