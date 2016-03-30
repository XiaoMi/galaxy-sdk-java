package org.apache.spark.streaming.talos

import com.xiaomi.infra.galaxy.rpc.thrift.Credential
import com.xiaomi.infra.galaxy.talos.consumer.SimpleConsumer
import com.xiaomi.infra.galaxy.talos.thrift.MessageAndOffset
import org.apache.spark.annotation.{DeveloperApi, Experimental}
import org.apache.spark.partial.{BoundedDouble, PartialResult}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.NextIterator
import org.apache.spark.{Logging, Partition, SparkContext, TaskContext}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
  * Created by jiasheng on 16-3-15.
  */
private[talos]
class TalosRDD[
R: ClassTag](
  sc: SparkContext,
  talosParams: Map[String, String],
  val offsetRanges: Array[OffsetRange],
  credential: Credential,
  messageHandler: MessageAndOffset => R
) extends RDD[R](sc, Nil) with Logging with HasOffsetRanges {

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

    private def fetchNumber = part.offsetRange.untilOffset - requestOffset + 1

    private def fetchBatch: Iterator[MessageAndOffset] = {
      val consumer: SimpleConsumer = tc.simpleConsumer(part.offsetRange.topic,
        part.offsetRange.partition)
      val resp = consumer.fetchMessage(requestOffset, fetchNumber.toInt)
      import scala.collection.JavaConverters._
      resp.iterator().asScala.dropWhile(_.messageOffset < requestOffset)
    }

    override protected def getNext(): R = {
      if (iter == null || !iter.hasNext) {
        iter = fetchBatch
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
