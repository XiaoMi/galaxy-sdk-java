package com.xiaomi.infra.galaxy.talos.spark.example

import org.apache.spark.SparkConf
import org.apache.spark.streaming.talos.TalosUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import com.xiaomi.infra.galaxy.rpc.thrift.{Credential, UserType}
import com.xiaomi.infra.galaxy.talos.client.TalosClientConfigKeys

/**
 * Created by jiasheng on 16-8-22.
 */
object TalosSparkDemo extends App {
  val batchSec = 5 // spark streaming batch interval;
  val appName = "SparkTalosTest" // spark streaming作业名称;
  val topic = "spark-talos-test" // 需要消费的talos topic名称；
  val talosServiceEndpoint = "http://staging-cnbj2-talos.api.xiaomi.net" //集群信息在这里查询：http://docs.api.xiaomi.net/talos/inner-cluster-info.html
  val keyId = "AKxxxx" //请在融合云"团队管理"服务中查询，密钥管理中的"ID"
  val keySecret = "xxxx" // 密钥管理中的"密钥内容（ SecretKey ）"

  val sparkConf = new SparkConf().setAppName(appName)
  val credential = new Credential()
    .setSecretKeyId(keyId).setSecretKey(keySecret).setType(UserType.DEV_XIAOMI)
  val ssc = new StreamingContext(sparkConf, Seconds(batchSec))

  // 对于之前已经开启了spark streaming checkpoint功能的作业，需要添加如下system property，才能使offset checkpoint生效；
  System.setProperty("spark.streaming.talos.offset.checkpoint.dir", "/user/u_wangjiasheng/offset_checkpoint/app_name")

  val talosDStream = TalosUtils.createDirectStream(
    ssc,
    Map(TalosClientConfigKeys.GALAXY_TALOS_SERVICE_ENDPOINT -> talosServiceEndpoint,
      "auto.offset.reset" -> "smallest",
      // 如下配置可以开启offset checkpoint，类似于talos高阶consumer的自动commit offset；
      "offset.checkpoint.dir" -> "/user/u_wangjiasheng/offset_checkpoint/app_name"),
    credential,
    Set(topic),
    mo => mo.message.getMessage)

  talosDStream.foreachRDD { rdd =>
    rdd.foreachPartition { partition =>
      partition.foreach { bytes =>
        val messageStr = new String(bytes, "UTF-8")
        println(messageStr)
      }
    }
  }

  ssc.start()
  ssc.awaitTermination()
}
