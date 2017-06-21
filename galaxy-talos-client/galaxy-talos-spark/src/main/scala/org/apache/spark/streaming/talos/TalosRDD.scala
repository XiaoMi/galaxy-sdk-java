package org.apache.spark.streaming.talos

import java.util

import com.xiaomi.infra.galaxy.rpc.thrift.Credential
import com.xiaomi.infra.galaxy.talos.client.TalosClientConfigKeys
import com.xiaomi.infra.galaxy.talos.consumer.SimpleConsumer
import com.xiaomi.infra.galaxy.talos.thrift.{ErrorCode, GalaxyTalosException, MessageAndOffset}
import org.apache.spark.annotation.{DeveloperApi, Experimental}
import org.apache.spark.partial.{BoundedDouble, PartialResult}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.NextIterator
import org.apache.spark.{Logging, Partition, SparkContext, SparkException, TaskContext}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

/**
  * Created by jiasheng on 16-3-15.
  */
private[talos]
class TalosRDD[
R: ClassTag](
  @transient sc: SparkContext,
  talosParams: Map[String, String],
  val offsetRanges: Array[OffsetRange],
  credential: Credential,
  messageHandler: MessageAndOffset => R
) extends RDD[R](sc, Nil) with Logging with HasOffsetRanges {

  private val applicationId = sc.applicationId
  private val maxRetries = sparkContext.getConf.getInt(
    "spark.streaming.talos.maxRetries", -1) // infinite retry
  private val backoffMs = sparkContext.getConf.getInt(
    "spark.streaming.talos.backoff.ms", 200)

  private def errBeginAfterEnd(part: TalosRDDPartition): String =
    s"Beginning offset ${part.offsetRange.fromOffset} >= ending offset" +
      s" ${part.offsetRange.untilOffset} for topic ${part.offsetRange.topic} " +
      s"partition ${part.offsetRange.partition}. Skipping!"

  private def errRanOutBeforeEnd(part: TalosRDDPartition): String =
    s"Ran out of messages before reaching ending offset ${part.offsetRange.untilOffset} for " +
      s"topic ${part.offsetRange.topic} partition ${part.offsetRange.partition} " +
      s"start ${part.offsetRange.fromOffset}. This should not happen, " +
      s"and indicates that messages may have been lost"

  private def errOvershotEnd(itemOffset: Long, part: TalosRDDPartition): String =
    s"Got ${itemOffset} > ending offset ${part.offsetRange.untilOffset} " +
      s"for topic ${part.offsetRange.topic} partition ${part.offsetRange.partition} " +
      s"start ${part.offsetRange.fromOffset}. This should not happen, " +
      s"and indicates a message may have been skipped"

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[R] = {
    val part = split.asInstanceOf[TalosRDDPartition]
    // As with empty partition, it's start offset is 0, end offset is -1, start > end.
    // assert(part.offsetRange.fromOffset <= part.offsetRange.untilOffset, errBeginAfterEnd(part))
    if (part.offsetRange.fromOffset >= part.offsetRange.untilOffset) {
      log.info(errBeginAfterEnd(part))
      Iterator.empty
    } else {
      new TalosRDDIterator(part, context)
    }
  }

  override protected def getPartitions: Array[Partition] = {
    offsetRanges.zipWithIndex.map { case (o, i) =>
      new TalosRDDPartition(i, o)
    }.toArray
  }


  @Experimental
  override def countApprox(
    timeout: Long,
    confidence: Double
  ): PartialResult[BoundedDouble] = {
    val c = count
    new PartialResult(new BoundedDouble(c, 1.0, c, c), true)
  }


  override def isEmpty(): Boolean = count == 0L


  override def take(num: Int): Array[R] = {
    val nonEmptyPartitions = this.partitions
      .map(_.asInstanceOf[TalosRDDPartition])
      .filter(_.count > 0)

    if (num < 1 || nonEmptyPartitions.size < 1) {
      return new Array[R](0)
    }

    // Determine in advance how many messages need to be taken from each partition
    val parts = nonEmptyPartitions.foldLeft(Map[Int, Int]()) { (result, part) =>
      val remain = num - result.values.sum
      if (remain > 0) {
        val taken = Math.min(remain, part.count)
        result + (part.index -> taken.toInt)
      } else {
        result
      }
    }

    val buf = new ArrayBuffer[R]
    val res = context.runJob(
      this,
      (tc: TaskContext, it: Iterator[R]) => it.take(parts(tc.partitionId)).toArray,
      parts.keys.toArray)
    res.foreach(buf ++= _)
    buf.toArray
  }

  override def count(): Long = offsetRanges.map(_.count).sum

  private class TalosRDDIterator(
    part: TalosRDDPartition,
    context: TaskContext) extends NextIterator[R] {

    context.addTaskCompletionListener { context => closeIfNeeded() }

    log.info(s"Computing topic ${part.offsetRange.topic}, " +
      s"partition ${part.offsetRange.partition} offsets" +
      s" ${part.offsetRange.fromOffset} -> ${part.offsetRange.untilOffset}")

    val tc = new TalosCluster(talosParams, credential)
    var requestOffset = part.offsetRange.fromOffset
    var iter: Iterator[MessageAndOffset] = null

    private def fetchNumber = part.offsetRange.untilOffset - requestOffset

    private def resetRequestOffset(retries: Int): Unit = {
      var result = tc.getEarliestOffsets(Set(part.offsetRange.topic))
      var i = retries

      while (result.isLeft && (i == -1 || i > 0)) {
        val errs = result.left.get
        if (errs.exists(t => t.isInstanceOf[GalaxyTalosException] &&
          fatalTalosErr.contains(t.asInstanceOf[GalaxyTalosException].errorCode))) {
          throw new SparkException(errs.mkString(","))
        }
        logWarning(s"Fetch offsets failed, will retry again after $backoffMs ms.\n" +
          s"$errs")
        Thread.sleep(backoffMs)
        result = tc.getEarliestOffsets(Set(part.offsetRange.topic))
        if (i > 0) {
          i -= 1
        }
      }

      result match {
        case Left(errs) =>
          throw new SparkException(errs.mkString(","))
        case Right(offsets) =>
          val earliestOffset = offsets(TopicPartition(part.offsetRange.topic,
            part.offsetRange.partition))
          require(requestOffset < earliestOffset,
            "Invalid MESSAGE_OFFSET_OUT_OF_RANGE: requested offset is larger than earliest offset.")
          logWarning(s"Reset request offset from $requestOffset to $earliestOffset, " +
            s"lost ${earliestOffset - requestOffset} messages.")
          requestOffset = math.min(earliestOffset, part.offsetRange.untilOffset)
      }
    }

    private val fatalTalosErr = Set(ErrorCode.TOPIC_NOT_EXIST, ErrorCode.PARTITION_NOT_EXIST,
      ErrorCode.INVALID_AUTH_INFO, ErrorCode.PERMISSION_DENIED_ERROR)

    private def fetchBatch(retries: Int): Iterator[MessageAndOffset] = {
      def fetchMessage = {
        val maxFetch = math.min(fetchNumber,
          TalosClientConfigKeys.GALAXY_TALOS_CONSUMER_MAX_FETCH_RECORDS_MAXIMUM)

        if(maxFetch <=0 ){
          new util.ArrayList[MessageAndOffset]()
        } else {
          val consumer: SimpleConsumer = tc.simpleConsumer(
            part.offsetRange.topic,
            part.offsetRange.partition,
            Some(s"${applicationId}-${part.offsetRange.topic}-${part.offsetRange.partition}"))
          consumer.fetchMessage(requestOffset, maxFetch.toInt)
        }
      }

      var i = retries
      var resp = Try(fetchMessage)

      while (resp.isFailure && (i == -1 || i > 0)) {
        val e = resp.failed.get
        e match {
          case gte: GalaxyTalosException =>
            if (fatalTalosErr.contains(gte.errorCode)) {
              throw gte
            } else if (gte.errorCode.equals(ErrorCode.MESSAGE_OFFSET_OUT_OF_RANGE)) {
              logWarning(s"Fetch messages failed, try to reset fetch offset.", e)
              resetRequestOffset(maxRetries)
            }
          case _: Exception =>
            logWarning(s"Fetch messages failed, will retry again after $backoffMs ms.", e)
            Thread.sleep(backoffMs)
        }
        resp = Try(fetchMessage)
        if (i > 0) {
          i -= 1
        }
      }

      resp match {
        case Success(messageAndOffsets) =>
          import scala.collection.JavaConverters._
          messageAndOffsets.iterator().asScala.dropWhile(_.messageOffset < requestOffset)
        case Failure(e) =>
          throw e
      }
    }

    override protected def getNext(): R = {
      if (iter == null || !iter.hasNext) {
        iter = fetchBatch(maxRetries)
      }
      if (!iter.hasNext) {
        assert(requestOffset == part.offsetRange.untilOffset, errRanOutBeforeEnd(part))
        finished = true
        null.asInstanceOf[R]
      } else {
        val item = iter.next()
        if (item.messageOffset >= part.offsetRange.untilOffset) {
          assert(item.messageOffset == part.offsetRange.untilOffset,
            errOvershotEnd(item.messageOffset, part))
          finished = true
          null.asInstanceOf[R]
        } else {
          requestOffset = item.messageOffset + 1
          messageHandler(item)
        }
      }
    }

    override protected def close(): Unit = {
      // talos consumer do not need to be closed.
    }
  }

}

private[talos]
object TalosRDD {
  def apply[R: ClassTag](
    sc: SparkContext,
    talosParams: Map[String, String],
    credential: Credential,
    fromOffsets: Map[TopicPartition, Long],
    untilOffsets: Map[TopicPartition, Long],
    messageHandler: MessageAndOffset => R
  ): TalosRDD[R] = {
    val offsetRanges = fromOffsets.map { case (tp, fo) =>
      val uo = untilOffsets(tp)
      OffsetRange(tp.topic, tp.partition, fo, uo)
    }.toArray

    new TalosRDD[R](sc, talosParams, offsetRanges, credential, messageHandler)
  }
}
