package com.xiaomi.infra.galaxy.talos.spark.example

import java.nio.charset.Charset

import com.xiaomi.infra.galaxy.rpc.thrift.{Credential, UserType}
import com.xiaomi.infra.galaxy.talos.client.TalosClientConfigKeys
import com.xiaomi.infra.galaxy.talos.thrift.MessageAndOffset
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.talos.TalosUtils

/**
  * Created by jiasheng on 16-8-23.
  */
object CustomMessageHandlerDemo {
  val batchSec = 5
  val appName = "SparkTalosTest"
  val topic = "spark-talos-test"
  val uri = "http://***"
  val key = "***"
  val secret = "***"

  val sparkConf = new SparkConf().setAppName(appName)
  val credential = new Credential()
    .setSecretKeyId(key).setSecretKey(secret).setType(UserType.DEV_XIAOMI)

  // custom content of InputDStream
  val messageHandler = (mo: MessageAndOffset) =>
    (mo.message.partitionKey, mo.messageOffset,
      new String(mo.message.message.array(), Charset.forName("UTF-8")))

  val ssc = TalosUtils.createStreamingContext(
    sparkConf,
    Seconds(batchSec),
    Map(TalosClientConfigKeys.GALAXY_TALOS_SERVICE_ENDPOINT -> uri,
      "auto.offset.reset" -> "smallest"),
    credential,
    Set(topic),
    messageHandler) { inputDstream => {
    inputDstream.foreachRDD(rdd => {
      // scalastyle:off println
      rdd.foreach(println)
      // scalastyle:on println
    })
  }
  }

  ssc.start()
  ssc.awaitTermination()
}
