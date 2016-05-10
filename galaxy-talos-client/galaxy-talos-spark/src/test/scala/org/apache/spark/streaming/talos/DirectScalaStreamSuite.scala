package org.apache.spark.streaming.talos

import java.io.File
import java.util.concurrent.atomic.AtomicLong

import com.xiaomi.infra.galaxy.talos.thrift.MessageAndOffset
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.scheduler.rate.RateEstimator
import org.apache.spark.streaming.scheduler._
import org.apache.spark.streaming.talos.DirectScalaStreamSuite.InputInfoCollector
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.util.Utils
import org.apache.spark.{Logging, SparkConf, SparkContext, SparkFunSuite}
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}

import scala.collection.mutable.ArrayBuffer
import scala.collection.{immutable, mutable}
import scala.concurrent.duration._

/**
  * Created by jiasheng on 16-3-21.
  */
class DirectScalaStreamSuite
  extends SparkFunSuite
    with BeforeAndAfter
    with BeforeAndAfterAll
    with Eventually
    with Logging {
  val sparkConf = new SparkConf()
    .setMaster("local[4]")
    .setAppName(this.getClass.getSimpleName)

  private var sc: SparkContext = _
  private var ssc: StreamingContext = _
  // checkpointing dir
  private var testDir: File = _

  private var talosTestUtils: TalosTestUtils = _


  override protected def beforeAll(): Unit = {
  }

  override protected def afterAll(): Unit = {
  }

  after {
    if (ssc != null) {
      ssc.stop()
      sc = null
    }
    if (sc != null) {
      sc.stop()
    }
    if (testDir != null) {
      Utils.deleteRecursively(testDir)
    }
  }

  test("basic stream receiving with multiple topics and smallest starting offset") {
    talosTestUtils = new TalosTestUtils(
      immutable.Map[String, String]("auto.offset.reset" -> "smallest"))

    val topics = Set("spark-talos-1", "spark-talos-2", "spark-talos-3")
    val partitionNum = 8
    val totalSent = 100
    topics.foreach { t =>
      talosTestUtils.deleteTopic(t)
      talosTestUtils.createTopic(t, partitionNum)
      talosTestUtils.sendMessages(t, (1 to totalSent).map(_.toString): _*)
    }

    ssc = new StreamingContext(sparkConf, Milliseconds(200))
    val stream = withClue("Error creating direct stream") {
      TalosUtils.createDirectStream(ssc, talosTestUtils.talosParams,
        talosTestUtils.credential, topics)
    }

    val allReceived =
      new ArrayBuffer[(String, String)] with mutable.SynchronizedBuffer[(String, String)]

    var offsetRanges: Array[OffsetRange] = null

    stream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }.foreachRDD { rdd =>
      for (o <- offsetRanges) {
        logInfo(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
      }
      val collected = rdd.mapPartitionsWithIndex { (i, iter) =>
        val off = offsetRanges(i)
        val all = iter.toSeq
        val partSize = all.size
        val rangeSize = off.untilOffset - off.fromOffset
        Iterator((partSize, rangeSize))
      }.collect()

      collected.foreach { case (partSize, rangeSize) =>
        assert(partSize === rangeSize, "offset ranges are wrong")
      }
    }
    stream.foreachRDD { rdd => allReceived ++= rdd.collect() }
    ssc.start()
    eventually(timeout(20000.milliseconds), interval(200.milliseconds)) {
      assert(allReceived.size == totalSent * topics.size,
        "didn't get expected number of messages, messages:\n" + allReceived.mkString("\n")
      )
    }
    ssc.stop()
  }

  test("receiving from largest starting offset") {
    talosTestUtils = new TalosTestUtils(immutable.Map[String, String](
      "auto.offset.reset" -> "largest"
    ))
    val topic = "spark-talos-largest"
    val message = "a"
    talosTestUtils.deleteTopic(topic)
    talosTestUtils.createTopic(topic, 8)
    talosTestUtils.sendMessages(topic, Seq.fill(10)(message): _*)

    var maxOffsetPartition: (TopicPartition, Long) = null
    eventually(timeout(10 seconds), interval(20 milliseconds)) {
      val latestOffsets = talosTestUtils.tc.getLatestOffsets(Set(topic))
      if (latestOffsets.isRight) {
        maxOffsetPartition = latestOffsets.right.get.maxBy(_._2)
        assert(maxOffsetPartition._2 > 1, "send message failed!")
      }
    }

    ssc = new StreamingContext(sparkConf, Milliseconds(200))
    val stream = withClue("Error creating direct stream") {
      TalosUtils.createDirectStream(ssc, talosTestUtils.talosParams,
        talosTestUtils.credential, Set(topic))
    }
    assert(
      stream.asInstanceOf[DirectTalosInputDStream[_]]
        .fromOffsets(maxOffsetPartition._1) >= maxOffsetPartition._2,
      "Start offset not from latest!"
    )

    val collectecData = new mutable.ArrayBuffer[String]() with mutable.SynchronizedBuffer[String]
    stream.map(_._2).foreachRDD { rdd => collectecData ++= rdd.collect() }
    ssc.start()
    val newMessage = "b"
    talosTestUtils.sendMessages(topic, Seq.fill(10)(newMessage): _*)
    eventually(timeout(10.seconds), interval(50.milliseconds)) {
      collectecData.contains("b")
    }
    assert(!collectecData.contains("a"), "Fetched outdated message!")
  }

  test("offset recovery") {
    talosTestUtils = new TalosTestUtils(immutable.Map[String, String](
      "auto.offset.reset" -> "smallest"
    ))
    testDir = Utils.createTempDir()
    val topic = "spark-talos-recovery"

    // recreate topic
    talosTestUtils.deleteTopic(topic)
    talosTestUtils.createTopic(topic, 8)

    // Send data to Talos and wait for it to be received
    def sendDataAndWaitForReceive(data: Seq[Int]): Unit = {
      val strings = data.map(_.toString)
      talosTestUtils.sendMessages(topic, strings: _*)
      eventually(timeout(10 seconds), interval(50 milliseconds)) {
        assert(strings.forall(DirectScalaStreamSuite.collectedData.contains))
      }
    }


    // Setup the streaming context
    ssc = new StreamingContext(sparkConf, Milliseconds(100))
    val talosStream = withClue("Error creating direct stream") {
      TalosUtils.createDirectStream(ssc, talosTestUtils.talosParams,
        talosTestUtils.credential, Set(topic))
    }
    val keyedStream = talosStream.map { case (_, v) => "key" -> v.toInt }
    val stateStream = keyedStream.updateStateByKey { (values: Seq[Int], state: Option[Int]) =>
      Some(values.sum + state.getOrElse(0))
    }
    ssc.checkpoint(testDir.getAbsolutePath)

    // This is to collect the raw data received from Talos
    talosStream.foreachRDD { (rdd, time) =>
      val data = rdd.map(_._2).collect()
      DirectScalaStreamSuite.collectedData ++= data
    }

    // This is ensure all the data is eventually receiving only once
    stateStream.foreachRDD { rdd =>
      rdd.collect().headOption.foreach { x => DirectScalaStreamSuite.total = x._2 }
    }
    ssc.start()

    // Send some data and wait for them to be received
    for (i <- (1 to 10).grouped(4)) {
      sendDataAndWaitForReceive(i)
    }

    // Verify that offset ranges were generated
    val offsetRangesBeforeStop = getOffsetRanges(
      talosStream.asInstanceOf[DirectTalosInputDStream[(String, String)]])
    assert(offsetRangesBeforeStop.size >= 1, "No offset ranges generated")
    assert(
      offsetRangesBeforeStop.head._2.forall(_.fromOffset === 0),
      "starting offset not zero"
    )
    ssc.stop()

    logInfo("====== RESTARTING ======")
    // Recover context from checkpoints
    ssc = new StreamingContext(testDir.getAbsolutePath)
    val recoveredStream = ssc.graph.getInputStreams().head.asInstanceOf[DStream[_]]

    // Verify offset ranges have been recovered
    val recoveredOffsetRanges = getOffsetRanges(
      recoveredStream.asInstanceOf[DirectTalosInputDStream[(String, String)]])
    assert(recoveredOffsetRanges.size > 0, "No offset ranges recovered")
    val earlierOffsetRangesAsSets = offsetRangesBeforeStop.map(x => (x._1, x._2.toSet))
    assert(
      recoveredOffsetRanges.forall { or =>
        earlierOffsetRangesAsSets.contains((or._1, or._2.toSet))
      },
      "Recovered ranges are not the same as the ones generated"
    )
    // Restart context, give more data and verify the total at the end
    // If the total is right that means each records has been received only once
    ssc.start()
    sendDataAndWaitForReceive(11 to 20)
    eventually(timeout(10 seconds), interval(50 milliseconds)) {
      assert(DirectScalaStreamSuite.total === (1 to 20).sum)
    }
    ssc.stop()
  }

  test("Direct Talos stream report input information") {
    talosTestUtils = new TalosTestUtils(immutable.Map[String, String](
      "auto.offset.reset" -> "smallest"
    ))
    val topic = "spark-talos-report-test"
    val totalSent = 100
    val messages = (1 to totalSent).map(_.toString)

    talosTestUtils.deleteTopic(topic)
    talosTestUtils.createTopic(topic, 8)
    talosTestUtils.sendMessages(topic, messages: _*)

    ssc = new StreamingContext(sparkConf, Milliseconds(200))
    val collector = new InputInfoCollector
    ssc.addStreamingListener(collector)

    val stream = withClue("Error creating direct stream") {
      TalosUtils.createDirectStream(ssc, talosTestUtils.talosParams,
        talosTestUtils.credential, Set(topic))
    }

    val allReceived =
      new ArrayBuffer[(String, String)] with mutable.SynchronizedBuffer[(String, String)]

    stream.foreachRDD(rdd => allReceived ++= rdd.collect())
    ssc.start()
    eventually(timeout(20000.milliseconds), interval(200.milliseconds)) {
      assert(allReceived.size === totalSent,
        "didn't get expected number of messages, messages:\n" + allReceived.mkString("\n"))

      // Calculate all the record number collected in the StreamingListener.
      assert(collector.numRecordsSubmitted.get() === totalSent)
      assert(collector.numRecordsStarted.get() === totalSent)
      assert(collector.numRecordsCompleted.get() === totalSent)
    }
    ssc.stop()
  }

  test("using rate controller") {
    talosTestUtils = new TalosTestUtils(immutable.Map[String, String](
      "auto.offset.reset" -> "smallest"
    ))
    val topic = "spark-talos-backpressure-test"
    talosTestUtils.deleteTopic(topic)
    talosTestUtils.createTopic(topic, 1)

    val batchIntervalMs = 100
    val estimator = new ConstantEstimator(100)
    //    val messages = (1 to 200).map(_.toString)
    //    talosTestUtils.sendMessages(topic, messages: _*)

    val sparkConf = new SparkConf()
      // Safe, even with streaming, because we're using the direct API.
      // Using 1 core is useful to make the test more predictable.
      .setMaster("local[1]")
      .setAppName(this.getClass.getSimpleName)
      .set("spark.streaming.talos.maxRatePerPartition", "100")

    // Setup the streaming context
    ssc = new StreamingContext(sparkConf, Milliseconds(batchIntervalMs))

    val talosStream = withClue("Error creating direct stream") {
      val fromOffsets = talosTestUtils.tc.getEarliestOffsets(Set(topic)).right.get
      val messageHandler =
        (mo: MessageAndOffset) => (mo.message.partitionKey, new String(mo.message.message.array()))
      new DirectTalosInputDStream[(String, String)](ssc, talosTestUtils.talosParams,
        talosTestUtils.credential, fromOffsets, messageHandler) {
        override protected[streaming] val rateController: Option[RateController] =
          Some(new DirectTalosRateController(id, estimator))
      }
    }

    val collectedData =
      new mutable.ArrayBuffer[Array[String]]() with mutable.SynchronizedBuffer[Array[String]]

    // Used for assertion failure messages.
    def dataToString: String =
      collectedData.map(_.mkString("[", ",", "]")).mkString("{", ",", "}")

    // This is to collect the raw data received from Talos
    talosStream.foreachRDD { (rdd, time) =>
      val data = rdd.map(_._2).collect()
      collectedData += data
    }

    ssc.start()

    // Try different rate limits.
    // Send data to Talos and wait for arrays of data to appear matching the rate.
    val timeoutSec: Int = 5
    Seq(100, 50, 20).foreach { rate =>
      collectedData.clear() // Empty this buffer on each pass.
      estimator.updateRate(rate) // Set a new rate.
    // Send messages.
    val messages = (1 to timeoutSec * 1000 / batchIntervalMs * rate).map(_.toString)
      talosTestUtils.sendMessages(topic, messages: _*)
      // Expect blocks of data equal to "rate", scaled by the interval length in secs.
      val expectedSize = Math.round(rate * batchIntervalMs * 0.001)
      eventually(timeout(timeoutSec seconds), interval(batchIntervalMs milliseconds)) {
        // Assert that rate estimator values are used to determine maxMessagesPerPartition.
        // Funky "-" in message makes the complete assertion message read better.
        assert(collectedData.exists(_.size == expectedSize),
          s" - No arrays of size $expectedSize for rate $rate found in $dataToString")
      }
      // Wait until received all data.
      var size = collectedData.size
      eventually(timeout(2 * timeoutSec seconds), interval(2 * batchIntervalMs milliseconds)) {
        assert(collectedData.size == size)
        size = collectedData.size
      }
    }
    ssc.stop()
  }

  /** Get the generated offset ranges from the DirectTalosStream */
  private def getOffsetRanges[R](talosStream: DirectTalosInputDStream[R]) = {
    talosStream.generatedRDDs.mapValues {
      rdd =>
        rdd.asInstanceOf[TalosRDD[R]].offsetRanges
    }.toSeq.sortBy(_._1)
  }
}

object DirectScalaStreamSuite {
  val collectedData = new mutable.ArrayBuffer[String]() with mutable.SynchronizedBuffer[String]
  @volatile var total = -1

  class InputInfoCollector extends StreamingListener {
    val numRecordsSubmitted = new AtomicLong(0L)
    val numRecordsStarted = new AtomicLong(0L)
    val numRecordsCompleted = new AtomicLong(0L)

    override def onBatchSubmitted(batchSubmitted: StreamingListenerBatchSubmitted): Unit = {
      numRecordsSubmitted.addAndGet(batchSubmitted.batchInfo.numRecords)
    }

    override def onBatchStarted(batchStarted: StreamingListenerBatchStarted): Unit = {
      numRecordsStarted.addAndGet(batchStarted.batchInfo.numRecords)
    }

    override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
      numRecordsCompleted.addAndGet(batchCompleted.batchInfo.numRecords)
    }
  }

}


private[streaming] class ConstantEstimator(@volatile private var rate: Long)
  extends RateEstimator {

  def updateRate(newRate: Long): Unit = {
    rate = newRate
  }

  def compute(
    time: Long,
    elements: Long,
    processingDelay: Long,
    schedulingDelay: Long): Option[Double] = Some(rate)
}
