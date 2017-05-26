package org.apache.spark.streaming.talos

import com.xiaomi.infra.galaxy.rpc.thrift.Credential
import com.xiaomi.infra.galaxy.talos.thrift.{ErrorCode, GalaxyTalosException, MessageAndOffset}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStreamCheckpointData, InputDStream}
import org.apache.spark.streaming.scheduler.rate.RateEstimator
import org.apache.spark.streaming.scheduler.{RateController, StreamInputInfo}
import org.apache.spark.streaming.talos.offset.HDFSOffsetDAO
import org.apache.spark.streaming.talos.perfcounter._
import org.apache.spark.streaming.{StreamingContext, Time}
import org.apache.spark.{Logging, SparkConf, SparkException}

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

/**
  * Created by jiasheng on 16-3-15.
  */
@SerialVersionUID(-3744996582858876937L)
private[streaming]
class DirectTalosInputDStream[R: ClassTag](
  @transient ssc_ : StreamingContext,
  val talosParams: Map[String, String],
  val credential: Credential,
  val fromOffsets: Map[TopicPartition, Long],
  messageHandler: MessageAndOffset => R
) extends InputDStream[R](ssc_) with Logging {

  private val maxRetries = context.sparkContext.getConf.getInt(
    "spark.streaming.talos.maxRetries", -1) // infinite retry
  private val backoffMs = context.sparkContext.getConf.getInt(
    "spark.streaming.talos.backoff.ms", 200)

  private def offsetDirOpt = talosParams.get("offset.checkpoint.dir") match {
    case Some(dir) => Some(dir)
    // for transiting from checkpoint app.
    case None => Option(System.getProperty("spark.streaming.talos.offset.checkpoint.dir", null))
  }
  @transient private var _offsetDao: Option[HDFSOffsetDAO] = null

  private def offsetDao: Option[HDFSOffsetDAO] = {
    if (_offsetDao == null) {
      _offsetDao = offsetDirOpt.map(offsetDir =>
        new HDFSOffsetDAO(
          offsetDir,
          ssc.sparkContext.hadoopConfiguration))
    }
    _offsetDao
  }
  private var committedTime = Option.empty[Time]

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

  private val fatalTalosErr = Set(ErrorCode.TOPIC_NOT_EXIST, ErrorCode.PARTITION_NOT_EXIST,
    ErrorCode.INVALID_AUTH_INFO, ErrorCode.PERMISSION_DENIED_ERROR)

  protected final def latestOffsets(retries: Int): Map[TopicPartition, Long] = {
    var i = retries
    var o = tc.getLatestOffsets(currentOffsets.keySet.map(_.topic))
    while (o.isLeft && (i == -1 || i > 0)) {
      val errs = o.left.get
      if (errs.exists(t => t.isInstanceOf[GalaxyTalosException] &&
        fatalTalosErr.contains(t.asInstanceOf[GalaxyTalosException].errorCode))) {
        throw new SparkException(errs.mkString(","))
      }
      logWarning(s"Fetch offsets failed, will retry again after $backoffMs ms.\n$errs")
      Thread.sleep(backoffMs)
      o = tc.getLatestOffsets(currentOffsets.keySet.map(_.topic))
      if (i > 0) {
        i -= 1
      }
    }
    o match {
      case Left(err) => throw new SparkException(err.toString)
      case Right(latestOffsets) =>
        assert(latestOffsets.size == currentOffsets.size,
          s"Partition number of ${currentOffsets.keySet.head.topic} changed! " +
            s"Previous: ${currentOffsets.size}; Current: ${o.right.get.size}")
        latestOffsets
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
    startPerfReporter()
  }

  override def stop(): Unit = {
    if (PerfReporter.isRunning) {
      PerfReporter.stop()
    }
    offsetDao.map(_.stop())
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

  override private[streaming] def clearMetadata(time: Time): Unit = {
    // clearMetadata is called when a batch is completed.
    tryCommitOffset(time, generatedRDDs.get(time).map(_.asInstanceOf[TalosRDD[_]]))
    super.clearMetadata(time)
  }

  private def tryCommitOffset(time: Time, rdd: Option[TalosRDD[_]]): Unit = {
    if (offsetDao.isEmpty || rdd.isEmpty) {
      return
    }
    try {
      val currentOffsets = rdd.get.offsetRanges.map { or =>
        TopicPartition(or.topic, or.partition) -> or.untilOffset
      }.toMap
      offsetDao.get.save(time, currentOffsets)
      committedTime = Option(time)
    } catch {
      case t: Throwable => logWarning("Commit offset failed.", t)
    }
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
      // restore lag reporter
      startPerfReporter()
    }
  }

  private[streaming]
  class DirectTalosRateController(id: Int, estimator: RateEstimator)
    extends RateController(id, estimator) {
    override protected def publish(rate: Long): Unit = ()
  }

  private def startPerfReporter() = {
    val topics = fromOffsets.map(_._1.topic).toSet
    PerfReporter.register(new LagPerfListener(context.sparkContext.conf))
    PerfReporter.register(new ActiveRDDsPerfListener(this, topics))
    PerfReporter.startIfEnabled(context.conf)
  }

  private[streaming]
  class LagPerfListener(sparkConf: SparkConf) extends PerfListener(sparkConf) {

    private def calcOffsetLag() = {
      if (generatedRDDs.isEmpty) {
        None
      } else {
        val minTime = generatedRDDs.keySet.min
        val offsetRange = generatedRDDs(minTime).asInstanceOf[TalosRDD[R]].offsetRanges
        val consumeOffsets = offsetRange.map(or =>
          (new TopicPartition(or.topic, or.partition), or.fromOffset)).toMap
        Try(latestOffsets(0)) match {
          case Success(offsets) =>
            Some(offsets.map {
              case (tp, lo) => (tp, lo - consumeOffsets(tp))
            })
          case Failure(e) =>
            logWarning("Get ConsumerOffsetLag info failed.", e)
            None
        }
      }
    }

    private def getPerfBean(
      topic: String,
      partitionOpt: Option[Int],
      timeStamp: Long,
      lag: Long) = {
      PerfBean(
        endpoint,
        "TalosOffsetLag",
        timeStamp,
        stepInSeconds,
        lag,
        CounterType.GAUGE,
        mutable.LinkedHashMap(
          "appName" -> appName,
          "user" -> user,
          "cluster" -> clusterName,
          "topic" -> topic,
          "partition" -> partitionOpt.getOrElse("all").toString)
      )
    }

    override def onGeneratePerf(): Seq[PerfBean] = {
      val perfBeans = mutable.Buffer.empty[PerfBean]
      calcOffsetLag().foreach(tpAndLag => {
        var lagSum = 0L
        val timeStamp = System.currentTimeMillis()
        tpAndLag.foreach { case (tp, lag) =>
          perfBeans += getPerfBean(tp.topic, Some(tp.partition), timeStamp, lag)
          lagSum += lag
        }
        perfBeans += getPerfBean(tpAndLag.head._1.topic, None, timeStamp, lagSum)
      })
      perfBeans
    }
  }
}
