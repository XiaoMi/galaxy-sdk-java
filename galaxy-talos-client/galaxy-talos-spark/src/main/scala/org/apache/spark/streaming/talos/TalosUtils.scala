package org.apache.spark.streaming.talos

import java.lang.{Integer => JInt, Long => JLong}
import java.nio.charset.Charset
import java.util.{List => JList, Map => JMap, Set => JSet}

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

import org.apache.spark.api.java.function.{Function => JFunction}
import org.apache.spark.streaming.api.java.{JavaInputDStream, JavaPairInputDStream, JavaStreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.talos.TalosCluster.Err
import org.apache.spark.streaming.talos.offset.HDFSOffsetDAO
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.{Logging, SparkConf, SparkException}

import com.xiaomi.infra.galaxy.rpc.thrift.Credential
import com.xiaomi.infra.galaxy.talos.thrift.MessageAndOffset

/**
 * Created by jiasheng on 16-3-15.
 */
object TalosUtils extends Logging {

  def createDirectStream(
      ssc: StreamingContext,
      talosParams: Map[String, String],
      credential: Credential,
      topics: Set[String]
  ): InputDStream[(String, String)] = {
    val messageHandler = (mo: MessageAndOffset) => (mo.message.partitionKey,
        new String(mo.message.message.array(), Charset.forName("UTF-8")))
    createDirectStream(ssc, talosParams, credential, topics, messageHandler)
  }

  def createDirectStream[T: ClassTag](
      ssc: StreamingContext,
      talosParams: Map[String, String],
      credential: Credential,
      topics: Set[String],
      messageHandler: MessageAndOffset => T
  ): InputDStream[T] = {
    val tc = new TalosCluster(talosParams, credential)
    createDirectStream(tc, ssc, talosParams, credential, topics, messageHandler)
  }

  // for test
  private[talos] def createDirectStream[T: ClassTag](
      tc: TalosCluster,
      ssc: StreamingContext,
      talosParams: Map[String, String],
      credential: Credential,
      topics: Set[String],
      messageHandler: MessageAndOffset => T
  ): InputDStream[T] = {

    val reset = talosParams.get("auto.offset.reset").map(_.toLowerCase).getOrElse("largest")
    val offsetDirOpt = talosParams.get("offset.checkpoint.dir")

    val restoredOffsetsOpt = try {
      offsetDirOpt.map(offsetDir =>
        new HDFSOffsetDAO(offsetDir, ssc.sparkContext.hadoopConfiguration)
      ).flatMap(dao => dao.restore())
    } catch {
      case t: Throwable => {
        throw new SparkException(s"Restore offsets from ${offsetDirOpt.get} failed.", t)
      }
    }

    val fromOffsets = restoredOffsetsOpt.map(Right(_)).getOrElse {
      reset match {
        case "smallest" => tc.getEarliestOffsets(topics)
        case "largest" => tc.getLatestOffsets(topics)
        case r => Left(new Err += new IllegalArgumentException(
          s"Invalid config for 'auto.offset.reset': $r"
        ))
      }
    }

    val result = fromOffsets.right.map(fo =>
      new DirectTalosInputDStream(ssc, talosParams, credential, fo, messageHandler)
    )
    TalosCluster.checkErrors(result)
  }

  def createDirectStream[T](
      jssc: JavaStreamingContext,
      recordClass: Class[T],
      talosParams: JMap[String, String],
      credential: Credential,
      topics: JSet[String],
      messageHandler: JFunction[MessageAndOffset, T]
  ): JavaInputDStream[T] = {
    implicit val recordCmt: ClassTag[T] = ClassTag(recordClass)

    val cleanedHandler = jssc.sparkContext.clean(messageHandler.call _)
    createDirectStream[T](
      jssc.ssc,
      Map(talosParams.toSeq: _*),
      credential,
      Set(topics.toSeq: _*),
      cleanedHandler)
  }

  def createDirectStream(
      jssc: JavaStreamingContext,
      talosParams: JMap[String, String],
      credential: Credential,
      topics: JSet[String]
  ): JavaPairInputDStream[String, String] = {
    createDirectStream(
      jssc.ssc,
      Map(talosParams.toSeq: _*),
      credential,
      Set(topics.toSeq: _*)
    )
  }

  /**
   * Create StreamingContext with checkpointing dir auto setting to app name.
   * Disable auto checkpointing by setting 'spark.streaming.talos.checkpointing.enable'
   * to false in SparkConf.
   *
   * @param conf          SparkConf
   * @param batchDuration batch duration
   * @param talosParams   parameters for talos client
   * @param credential    credential
   * @param topics        talos topics
   * @param dStreamFunc   function to construct DStream. Refer to
   *                      http://docs.scala-lang.org/style/declarations.html
   *                      #multiple-parameter-lists for more info about
   *                      multiple parameter lists.
   * @return StreamingContext created or restored.
   */
  def createStreamingContext(
      conf: SparkConf,
      batchDuration: Duration,
      talosParams: Map[String, String],
      credential: Credential,
      topics: Set[String]
  )(
      dStreamFunc: InputDStream[(String, String)] => Unit
  ): StreamingContext = {
    val messageHandler = (mo: MessageAndOffset) => (mo.message.partitionKey,
        new String(mo.message.message.array(), Charset.forName("UTF-8")))
    createStreamingContext(conf, batchDuration, talosParams, credential, topics, messageHandler)(
      dStreamFunc
    )
  }

  /**
   * Create StreamingContext with checkpointing dir auto setting to app name.
   * Disable auto checkpointing by setting 'spark.streaming.talos.checkpointing.enable'
   * to false in SparkConf.
   *
   * @param conf           SparkConf
   * @param batchDuration  batch duration
   * @param talosParams    parameters for talos client
   * @param credential     credential
   * @param topics         talos topics
   * @param messageHandler custom message handler. Demo:
   *                       [[com.xiaomi.infra.galaxy.talos.spark.example.CustomMessageHandlerDemo]]
   * @param dStreamFunc    function to construct DStream. Refer to
   *                       http://docs.scala-lang.org/style/declarations.html
   *                       #multiple-parameter-lists for more info about multiple parameter lists.
   * @return StreamingContext created or restored.
   */
  def createStreamingContext[T: ClassTag](
      conf: SparkConf,
      batchDuration: Duration,
      talosParams: Map[String, String],
      credential: Credential,
      topics: Set[String],
      messageHandler: MessageAndOffset => T
  )(
      dStreamFunc: InputDStream[T] => Unit
  ): StreamingContext = {
    StreamingContext.getOrCreate(conf.get("spark.app.name"), () => {
      val ssc = new StreamingContext(conf, batchDuration)
      autosetCheckpointing(ssc)
      dStreamFunc(createDirectStream(ssc, talosParams, credential, topics, messageHandler))
      ssc
    })
  }

  private def autosetCheckpointing(ssc: StreamingContext) = {
    val enableCheckpointingConfigKey = "spark.streaming.talos.checkpointing.enable"
    val enableCheckpointing = ssc.sparkContext.getConf.getBoolean(
      enableCheckpointingConfigKey, true)

    if (!ssc.isCheckpointingEnabled && enableCheckpointing) {
      logWarning("We have automatically set checkpointing dir to your app name: " +
          s"${ssc.sparkContext.appName}. You can disable this by setting " +
          s"'$enableCheckpointingConfigKey' to false in SparkConf.")
      ssc.checkpoint(ssc.sparkContext.appName)
    }
  }
}
