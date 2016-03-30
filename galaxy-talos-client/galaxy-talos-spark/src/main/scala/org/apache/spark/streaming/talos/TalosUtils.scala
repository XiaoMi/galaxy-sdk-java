package org.apache.spark.streaming.talos

import java.nio.charset.Charset
import java.lang.{Integer => JInt, Long => JLong}
import java.util.{List => JList, Map => JMap, Set => JSet}

import com.xiaomi.infra.galaxy.rpc.thrift.Credential
import com.xiaomi.infra.galaxy.talos.thrift.MessageAndOffset
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.api.java.function.{Function => JFunction}
import org.apache.spark.streaming.api.java.{JavaInputDStream, JavaPairInputDStream, JavaStreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.talos.TalosCluster.Err

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

/**
  * Created by jiasheng on 16-3-15.
  */
object TalosUtils {

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
    val reset = talosParams.get("auto.offset.reset").map(_.toLowerCase).getOrElse("largest")

    val fromOffsets = reset match {
      case "smallest" => tc.getEarliestOffsets(topics)
      case "largest" => tc.getLatestOffsets(topics)
      case r => Left(new Err += new IllegalArgumentException(
        s"Invalid config for 'auto.offset.reset': $r"
      ))
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
}
