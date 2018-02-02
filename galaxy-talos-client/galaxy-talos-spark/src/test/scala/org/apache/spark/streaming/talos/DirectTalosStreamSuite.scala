package org.apache.spark.streaming.talos

import java.io.File
import java.nio.charset.Charset
import java.util.concurrent.atomic.AtomicLong
import java.util.{List => JList, Map => JMap, Set => JSet}

import scala.collection.mutable.ArrayBuffer
import scala.collection.{immutable, mutable}
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.scheduler._
import org.apache.spark.streaming.scheduler.rate.RateEstimator
import org.apache.spark.streaming.talos.DirectTalosStreamSuite.InputInfoCollector
import org.apache.spark.streaming.{Milliseconds, StreamingContext, Time}
import org.apache.spark.util.Utils
import org.apache.spark.{SparkConf, SparkContext}
import org.mockito.Matchers._
import org.mockito.Mockito._

import com.xiaomi.infra.galaxy.talos.admin.TalosAdmin
import com.xiaomi.infra.galaxy.talos.client.TalosClientConfig
import com.xiaomi.infra.galaxy.talos.thrift.{GetTopicOffsetRequest, MessageAndOffset, OffsetInfo}

/**
 * Created by jiasheng on 16-3-21.
 */
class DirectTalosStreamSuite extends TalosClusterSuite {
  val sparkConf = new SparkConf()
      .setMaster("local[4]")
      .setAppName(this.getClass.getSimpleName)

  private var sc: SparkContext = _
  private var ssc: StreamingContext = _
  // checkpointing dir
  private var testDir: File = _

  private var talosTestUtils: TalosTestUtils = _

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

  // basic talos operation
  test("cache creating instance") {
    val topic = "cache-creating-instance"
    talosTestUtils = new TalosTestUtils(uri, immutable.Map[String, String]())
    talosTestUtils.deleteTopic(topic)
    talosTestUtils.createTopic(topic, 1)
    talosTestUtils.sendMessagesAndWaitForReceive(topic, "message", "message")

    val config = talosTestUtils.tc.config
    assert(config.equals(talosTestUtils.tc.config), "Not using cached Configuration instance")
    val admin = talosTestUtils.tc.admin()
    assert(admin.equals(talosTestUtils.tc.admin()), "Not using cached TalosAdmin instance")
    val resourceName = talosTestUtils.tc.topicResourceName(topic)
    assert(resourceName.equals(talosTestUtils.tc.topicResourceName(topic)))
    val simpleConsumer = talosTestUtils.tc.simpleConsumer(topic, 0)
    assert(simpleConsumer.equals(talosTestUtils.tc.simpleConsumer(topic, 0)))
  }

  test("earliest offset api") {
    val topic = "earliest-offset-api"
    talosTestUtils = new TalosTestUtils(uri, immutable.Map[String, String]())
    talosTestUtils.deleteTopic(topic)
    talosTestUtils.createTopic(topic, 1)
    talosTestUtils.sendMessagesAndWaitForReceive(topic, "message", "message")

    val offset = talosTestUtils.tc.getEarliestOffsets(Set(topic)).right.get
    assert(offset.head._2 === 0, "didn't get earliest offset")
  }

  test("latest offset api") {
    val topic = "latest-offset-api"
    talosTestUtils = new TalosTestUtils(uri, immutable.Map[String, String]())
    talosTestUtils.deleteTopic(topic)
    talosTestUtils.createTopic(topic, 1)
    talosTestUtils.sendMessagesAndWaitForReceive(topic, "message", "message")

    val offset = talosTestUtils.tc.getLatestOffsets(Set(topic)).right.get
    assert(offset.head._2 === 2, "didn't get latest offset")
  }

  test("invalid offset info") {
    talosTestUtils = new TalosTestUtils(uri, immutable.Map[String, String]())
    val topics = Set("invalid-offset-info")
    val partitionNum = 1
    topics.foreach { t =>
      talosTestUtils.deleteTopic(t)
      talosTestUtils.createTopic(t, partitionNum)
    }

    // spy
    val mockTalosAdmin = spy(
      new TalosAdmin(new TalosClientConfig(talosTestUtils.tc.config), talosTestUtils.tc.credential))
    talosTestUtils.tc._talosAdmin = mockTalosAdmin

    // stub
    val invalidOffsetInfo = new OffsetInfo(0)
    invalidOffsetInfo.setStartOffset(-1)
    invalidOffsetInfo.setEndOffset(-1)
    val invalidOffsetInfoList = List(invalidOffsetInfo)
    import scala.collection.JavaConverters._
    doReturn(invalidOffsetInfoList.toList.asJava).when(mockTalosAdmin).getTopicOffset(any(classOf[GetTopicOffsetRequest]))

    // verify
    ssc = new StreamingContext(sparkConf, Milliseconds(200))
    Try {
      val messageHandler = (mo: MessageAndOffset) => (mo.message.partitionKey,
          new String(mo.message.message.array(), Charset.forName("UTF-8")))
      TalosUtils.createDirectStream(talosTestUtils.tc, ssc, talosTestUtils.talosParams,
        talosTestUtils.credential, topics, messageHandler)
    } match {
      case Success(_) => assert(false, "createDirectStream should failed in case of invalid OffsetInfo.")
      case Failure(e) => assert(e.isInstanceOf[java.lang.AssertionError] && e.getMessage.contains("Invalid OffsetInfo"),
        s"Exception should be java.lang.AssertionError")
    }
  }

  test("latest offset should not be smaller than current offset") {
    talosTestUtils = new TalosTestUtils(uri, immutable.Map[String, String]())
    val topics = Set("latest-offset-should-not-xxxx")
    val partitionNum = 1
    topics.foreach { t =>
      talosTestUtils.deleteTopic(t)
      talosTestUtils.createTopic(t, partitionNum)
    }

    // spy
    val mockTalosAdmin = spy(
      new TalosAdmin(new TalosClientConfig(talosTestUtils.tc.config), talosTestUtils.tc.credential))
    talosTestUtils.tc._talosAdmin = mockTalosAdmin

    // stub
    val partitionEndOffset = 100
    val partitionOffsetInfo = new OffsetInfo(0)
    partitionOffsetInfo.setStartOffset(0)
    partitionOffsetInfo.setEndOffset(partitionEndOffset)
    val offsetInfoList = List(partitionOffsetInfo)
    import scala.collection.JavaConverters._
    doReturn(offsetInfoList.toList.asJava).when(mockTalosAdmin).getTopicOffset(any(classOf[GetTopicOffsetRequest]))

    ssc = new StreamingContext(sparkConf, Milliseconds(200))
    val talosDstream: DirectTalosInputDStream[(String, String)] = withClue("Error creating direct stream") {
      TalosUtils.createDirectStream(ssc, talosTestUtils.talosParams,
        talosTestUtils.credential, topics)
    }.asInstanceOf[DirectTalosInputDStream[(String, String)]]
    // set current offset larger than latest offset
    talosDstream.currentOffsets = Map {
      TopicPartition(topics.iterator.next(), 0) -> (partitionEndOffset + 10)
    }

    // verify
    Try {
      talosDstream.compute(Time(1000))
    } match {
      case Success(_) => assert(false, "DirectTalosInputDStream should throw exception in case of invalid latest offset.")
      case Failure(e) => {
        assert(e.isInstanceOf[java.lang.AssertionError] && e.getMessage.contains("Invalid"),
          "Exception should be java.lang.AssertionError")
        logInfo(s"Expected exception: $e")
      }
    }
  }

  test("RDD from offset should not be smaller than partition start offset") {
    talosTestUtils = new TalosTestUtils(uri, immutable.Map[String, String]())
    val topics = Set("rdd-from-offset-should-not-xxxx")
    val partitionNum = 1
    topics.foreach { t =>
      talosTestUtils.deleteTopic(t)
      talosTestUtils.createTopic(t, partitionNum)
    }

    // spy
    val mockTalosAdmin = spy(
      new TalosAdmin(new TalosClientConfig(talosTestUtils.tc.config), talosTestUtils.tc.credential))
    talosTestUtils.tc._talosAdmin = mockTalosAdmin

    // stub
    val partitionStartOffset = 100
    val partitionOffsetInfo = new OffsetInfo(0)
    partitionOffsetInfo.setStartOffset(partitionStartOffset)
    partitionOffsetInfo.setEndOffset(partitionStartOffset + 10)
    val offsetInfoList = List(partitionOffsetInfo)
    import scala.collection.JavaConverters._
    doReturn(offsetInfoList.toList.asJava).when(mockTalosAdmin).getTopicOffset(any(classOf[GetTopicOffsetRequest]))

    ssc = new StreamingContext(sparkConf, Milliseconds(Long.MaxValue))
    val talosDstream: DirectTalosInputDStream[(String, String)] = withClue("Error creating direct stream") {
      TalosUtils.createDirectStream(ssc, talosTestUtils.talosParams,
        talosTestUtils.credential, topics)
    }.asInstanceOf[DirectTalosInputDStream[(String, String)]]
    talosDstream.tc._talosAdmin = mockTalosAdmin
    // set current offset smaller than partition start offset
    talosDstream.currentOffsets = Map {
      TopicPartition(topics.iterator.next(), 0) -> (partitionStartOffset - 10)
    }

    // verify
    talosDstream.foreachRDD(rdd => rdd.take(1))
    ssc.start()
    val talosRDD: TalosRDD[(String, String)] = talosDstream.compute(Time(1000)).get.asInstanceOf[TalosRDD[(String, String)]]
    ssc.stop()
    assert(talosRDD.offsetRanges(0).fromOffset == partitionStartOffset, "fromOffset should be reset to partition start offset")
  }

  test("partition number increased") {
    talosTestUtils = new TalosTestUtils(uri, immutable.Map[String, String]())
    val topics = Set("partition-number-increased")
    val partitionNum = 1
    topics.foreach { t =>
      talosTestUtils.deleteTopic(t)
      talosTestUtils.createTopic(t, partitionNum)
    }

    // spy
    val mockTalosAdmin = spy(
      new TalosAdmin(new TalosClientConfig(talosTestUtils.tc.config), talosTestUtils.tc.credential))
    talosTestUtils.tc._talosAdmin = mockTalosAdmin

    // stub
    val partitionStartOffset0 = 100
    val partitionOffsetInfo0 = new OffsetInfo(0)
    partitionOffsetInfo0.setStartOffset(partitionStartOffset0)
    partitionOffsetInfo0.setEndOffset(partitionStartOffset0 + 10)
    val partitionStartOffset1 = 10
    val partitionOffsetInfo1 = new OffsetInfo(1)
    partitionOffsetInfo1.setStartOffset(partitionStartOffset1)
    partitionOffsetInfo1.setEndOffset(partitionStartOffset1 + 10)
    val offsetInfoList = List(partitionOffsetInfo0, partitionOffsetInfo1)
    import scala.collection.JavaConverters._
    doReturn(offsetInfoList.toList.asJava).when(mockTalosAdmin).getTopicOffset(any(classOf[GetTopicOffsetRequest]))

    ssc = new StreamingContext(sparkConf, Milliseconds(Long.MaxValue))
    val talosDstream: DirectTalosInputDStream[(String, String)] = withClue("Error creating direct stream") {
      TalosUtils.createDirectStream(ssc, talosTestUtils.talosParams,
        talosTestUtils.credential, topics)
    }.asInstanceOf[DirectTalosInputDStream[(String, String)]]
    talosDstream.tc._talosAdmin = mockTalosAdmin
    // set current offset smaller than partition start offset
    talosDstream.currentOffsets = Map {
      TopicPartition(topics.iterator.next(), 0) -> partitionStartOffset0
    }

    // verify
    talosDstream.foreachRDD(rdd => rdd.take(1))
    ssc.start()
    val talosRDD: TalosRDD[(String, String)] = talosDstream.compute(Time(1000)).get.asInstanceOf[TalosRDD[(String, String)]]
    ssc.stop()
    assert(talosRDD.offsetRanges.size == 2 &&
        // TalosRDD should contains new partition.
        talosRDD.offsetRanges.contains(OffsetRange(topics.iterator.next(), 1, partitionOffsetInfo1.startOffset, partitionOffsetInfo1.endOffset + 1)),
      "fromOffset should be reset to partition start offset")
  }

  test("basic stream receiving with multiple topics and smallest starting offset") {
    talosTestUtils = new TalosTestUtils(uri,
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
    talosTestUtils = new TalosTestUtils(uri,
      immutable.Map[String, String]("auto.offset.reset" -> "largest"))
    val topic = "spark-talos-largest"
    val message = "a"
    talosTestUtils.deleteTopic(topic)
    talosTestUtils.createTopic(topic, 2)
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
    talosTestUtils = new TalosTestUtils(uri,
      immutable.Map[String, String]("auto.offset.reset" -> "smallest"))
    testDir = Utils.createTempDir()
    val topic = "spark-talos-recovery"

    // recreate topic
    talosTestUtils.deleteTopic(topic)
    talosTestUtils.createTopic(topic, 2)

    // Send data to Talos and wait for it to be received
    def sendDataAndWaitForReceive(data: Seq[Int]): Unit = {
      val strings = data.map(_.toString)
      talosTestUtils.sendMessages(topic, strings: _*)
      eventually(timeout(10 seconds), interval(50 milliseconds)) {
        assert(strings.forall(DirectTalosStreamSuite.collectedData.contains))
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
      DirectTalosStreamSuite.collectedData ++= data
    }

    // This is ensure all the data is eventually receiving only once
    stateStream.foreachRDD { rdd =>
      rdd.collect().headOption.foreach { x => DirectTalosStreamSuite.total = x._2 }
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
      assert(DirectTalosStreamSuite.total === (1 to 20).sum)
    }
    ssc.stop()
  }

  test("Direct Talos stream report input information") {
    talosTestUtils = new TalosTestUtils(uri,
      immutable.Map[String, String]("auto.offset.reset" -> "smallest"))
    val topic = "spark-talos-report-test"
    val totalSent = 100
    val messages = (1 to totalSent).map(_.toString)

    talosTestUtils.deleteTopic(topic)
    talosTestUtils.createTopic(topic, 2)
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
    talosTestUtils = new TalosTestUtils(uri,
      immutable.Map[String, String]("auto.offset.reset" -> "smallest"))
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
      estimator.updateRate(rate)
      // Set a new rate.
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

object DirectTalosStreamSuite {
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
