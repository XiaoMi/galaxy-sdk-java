package org.apache.spark.streaming.talos

import com.xiaomi.infra.galaxy.rpc.thrift.Credential
import com.xiaomi.infra.galaxy.talos.thrift.{MessageAndOffset}
import org.apache.spark.{SparkException, Logging}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.scheduler.{StreamInputInfo, RateController}
import org.apache.spark.streaming.scheduler.rate.RateEstimator
import org.apache.spark.streaming.{Time, StreamingContext}
import org.apache.spark.streaming.dstream.{DStreamCheckpointData, InputDStream}

import scala.annotation.tailrec
import scala.reflect.ClassTag

/**
  * Created by jiasheng on 16-3-15.
  */
private[streaming]
class DirectTalosInputDStream[R: ClassTag](
  @transient ssc_ : StreamingContext,
  val talosParams: Map[String, String],
  val credential: Credential,
  val fromOffsets: Map[TopicPartition, Long],
  messageHandler: MessageAndOffset => R
) extends InputDStream[R](ssc_) with Logging {

  val maxRetries = context.sparkContext.getConf.getInt(
    "spark.streaming.talos.maxRetries", 1)
  val backoffMs = context.sparkContext.getConf.getInt(
    "spark.streaming.talos.backoff.ms", 200)

  // Keep this consistent with how other streams are named (e.g. "Flume polling stream [2]")
  private[streaming] override def name: String = s"Talos direct stream [$id]"


  override private[streaming] val checkpointData: DStreamCheckpointData[R] =
    new DirectTalosInputDStreamCheckpointData

  /**
    * Asynchoronusly maintains & sends new rate limits to the receiver through the receiver tracker.
    */
  override protected[streaming] val rateController: Option[RateController] = {
    if (RateController.isBackPressureEnabled(ssc.conf)) {
      Some(new DirectTalosRateController(id,
        RateEstimator.create(ssc.conf, ssc_.graph.batchDuration)))
    } else {
      None
    }
  }

  protected def maxMessagesPerPartition: Option[Long] = {
    val estimatedRateLimit = rateController.map(_.getLatestRate().toInt)
    val numPartitions = currentOffsets.keys.size

    val effectiveRateLimitPerPartition = estimatedRateLimit
      .filter(_ > 0)
      .map { limit =>
        if (maxRateLimitPerPartition > 0) {
          Math.min(maxRateLimitPerPartition, (limit / numPartitions))
        } else {
          limit / numPartitions
        }
      }.getOrElse(maxRateLimitPerPartition)

    if (effectiveRateLimitPerPartition > 0) {
      val secsPerBatch = context.graph.batchDuration.milliseconds.toDouble / 1000
      Some((secsPerBatch * effectiveRateLimitPerPartition).toLong)
    } else {
      None
    }
  }

  protected var currentOffsets = fromOffsets

  protected val tc = new TalosCluster(talosParams, credential)

  // TODO: talos support throttle control!
  private val maxRateLimitPerPartition: Int = context.sparkContext.getConf.getInt(
    "spark.streaming.talos.maxRatePerPartition", 0)


  @tailrec
  protected final def latestOffsets(retries: Int): Map[TopicPartition, Long] = {
    val o = tc.getLatestOffsets(currentOffsets.keySet.map(_.topic))
    // Either.fold would confuse @tailrec, do it manually
    if (o.isLeft) {
      val err = o.left.get.toString
      if (retries <= 0) {
        throw new SparkException(err)
      } else {
        log.error(err)
        Thread.sleep(backoffMs)
        latestOffsets(retries - 1)
      }
    } else {
      assert(o.right.get.size == currentOffsets.size,
        s"Partition number of ${currentOffsets.keySet.head.topic} changed! " +
          s"Previous: ${currentOffsets.size}; Current: ${o.right.get.size}")
      o.right.get
    }
  }

  // limits the maximum number of messages per partition
  protected def clamp(
    latestOffsets: Map[TopicPartition, Long]): Map[TopicPartition, Long] = {
    maxMessagesPerPartition.map { mmp =>
      latestOffsets.map { case (tp, offset) =>
        tp -> Math.min(currentOffsets(tp) + mmp, offset)
      }
    }.getOrElse(latestOffsets)
  }

  override def start(): Unit = {
  }

  override def stop(): Unit = {
  }

  override def compute(validTime: Time): Option[RDD[R]] = {
    val untilOffsets = clamp(latestOffsets(maxRetries))
      // untilOffset must not be smaller than fromOffset.
      .map { case (tp, uo) => tp -> Math.max(currentOffsets(tp), uo) }
    val rdd = TalosRDD[R](context.sparkContext, talosParams, credential,
      currentOffsets, untilOffsets, messageHandler)

    // Report the record number and metadata of this batch interval to InputInfoTracker.
    val offsetRanges = currentOffsets.map { case (tp, fo) =>
      val uo = untilOffsets(tp)
      OffsetRange(tp.topic, tp.partition, fo, uo)
    }
    val description = offsetRanges.filter { offsetRange =>
      // Don't display empty ranges.
      offsetRange.fromOffset != offsetRange.untilOffset
    }.map { offsetRange =>
      s"topic: ${offsetRange.topic}\tpartition: ${offsetRange.partition}\t" +
        s"offsets: ${offsetRange.fromOffset} to ${offsetRange.untilOffset}"
    }.mkString("\n")
    // Copy offsetRanges to immutable.List to prevent from being modified by the user
    val metadata = Map(
      "offsets" -> offsetRanges.toList,
      StreamInputInfo.METADATA_KEY_DESCRIPTION -> description)
    val inputInfo = StreamInputInfo(id, rdd.count, metadata)
    ssc.scheduler.inputInfoTracker.reportInfo(validTime, inputInfo)

    currentOffsets = untilOffsets.map(kv => kv._1 -> kv._2)
    Some(rdd)
  }


  private[streaming]
  class DirectTalosInputDStreamCheckpointData extends DStreamCheckpointData(this) {
    def batchForTime: scala.collection.mutable.HashMap[Time, Array[(String, Int, Long, Long)]] = {
      data.asInstanceOf[scala.collection.mutable.HashMap[Time, Array[OffsetRange.OffsetRangeTuple]]]
    }

    override def update(time: Time): Unit = {
      batchForTime.clear()
      generatedRDDs.foreach { kv =>
        val a = kv._2.asInstanceOf[TalosRDD[R]].offsetRanges.map(_.toTuple)
        batchForTime += kv._1 -> a
      }
    }

    override def cleanup(time: Time): Unit = {}

    override def restore(): Unit = {
      // this is assuming that the topics don't change during execution, which is true currently
      batchForTime.toSeq.sortBy(_._1)(Time.ordering).foreach { case (t, b) =>
        logInfo(s"Restoring TalosRDD for time $t ${b.mkString("[", ",", "]")}")
        generatedRDDs += t -> new TalosRDD[R](
          context.sparkContext, talosParams, b.map(OffsetRange(_)), credential, messageHandler)
      }
    }
  }

  private[streaming]
  class DirectTalosRateController(id: Int, estimator: RateEstimator)
    extends RateController(id, estimator) {
    override protected def publish(rate: Long): Unit = ()
  }

}
