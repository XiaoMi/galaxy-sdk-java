package org.apache.spark.streaming.talos

import java.util.Properties

import com.xiaomi.infra.galaxy.rpc.thrift.Credential
import com.xiaomi.infra.galaxy.talos.admin.TalosAdmin
import com.xiaomi.infra.galaxy.talos.client.TalosClientConfig
import com.xiaomi.infra.galaxy.talos.consumer.{SimpleConsumer, TalosConsumerConfig}
import com.xiaomi.infra.galaxy.talos.thrift._
import org.apache.spark.streaming.talos.TalosCluster.Offset.Offset
import org.apache.spark.streaming.talos.TalosCluster.{Err, Offset}
import org.apache.spark.{Logging, SparkException}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Created by jiasheng on 16-3-15.
 */
@SerialVersionUID(-995673360675374997L)
private[spark]
class TalosCluster(
  val talosParams: Map[String, String],
  val credential: Credential
) extends Serializable with Logging {
  @transient
  private var _config: Properties = null
  @transient
  private var _talosAdmin: TalosAdmin = null
  @transient
  private var _cacheSimpleConsumer: mutable.Map[TopicPartition, SimpleConsumer] = null
  @transient
  private var _topicResourceNames: mutable.Map[String, TopicTalosResourceName] = null

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
        partitionOffsets.asScala.map(po => {
          if (po.errorCode != null) {
            val exception = new GalaxyTalosException()
            exception.setErrorCode(po.errorCode)
            exception.setErrMsg(po.errorMsg)
            throw exception
          }
          logInfo(s"(${topic}, ${po.partitionId}) OffsetRange: " +
            s"${po.startOffset} -> ${po.endOffset}")
          new TopicPartition((topic, po.partitionId)) ->
            (offset match {
              case Offset.Earliest => po.startOffset
              // in Kafka, latest offset is LogEndOffset:
              // The offset of the next message that WILL be appended to the log
              case Offset.Latest => po.endOffset + 1
            })
        }
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
