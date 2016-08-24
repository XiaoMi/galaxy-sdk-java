package com.xiaomi.infra.galaxy.talos.spark.example

import com.xiaomi.infra.galaxy.rpc.thrift.{Credential, UserType}
import com.xiaomi.infra.galaxy.talos.client.TalosClientConfigKeys
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.talos.{HasOffsetRanges, OffsetRange, TalosUtils}

/**
  * Created by jiasheng on 16-8-22.
  */
object TalosSparkDemo extends App {
  val batchSec = 5
  val appName = "SparkTalosTest"
  val topic = "spark-talos-test"
  val uri = "http://***"
  val key = "***"
  val secret = "***"

  val sparkConf = new SparkConf().setAppName(appName)
  val credential = new Credential()
    .setSecretKeyId(key).setSecretKey(secret).setType(UserType.DEV_XIAOMI)


  val ssc = TalosUtils.createStreamingContext(
    sparkConf,
    Seconds(batchSec),
    Map(TalosClientConfigKeys.GALAXY_TALOS_SERVICE_ENDPOINT -> uri,
      "auto.offset.reset" -> "smallest"),
    credential,
    Set(topic)) { inputDstream => {
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
