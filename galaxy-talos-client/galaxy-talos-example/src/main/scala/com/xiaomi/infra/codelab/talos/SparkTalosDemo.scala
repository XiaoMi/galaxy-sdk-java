package com.xiaomi.infra.codelab.talos

import java.nio.ByteBuffer
import java.util
import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

import com.xiaomi.infra.galaxy.rpc.thrift.{Credential, UserType}
import com.xiaomi.infra.galaxy.talos.thrift.Message

object SparkTalosDemo {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: NetworkWordCount <hostname> <port>")
      System.exit(1)
    }

    // Talos相关配置
    val topicName = "myTopic"
    val talosServiceEndpoint = "http://xxxx-talos.api.xiaomi.net"
    val accessKey = "myAccessKey"
    val accessSecret = "myAccessSecret"

    val talosProperties = new Properties()
    talosProperties.setProperty("galaxy.talos.service.endpoint", talosServiceEndpoint)
    val credential = new Credential()
    credential.setSecretKeyId(accessKey).setSecretKey(accessSecret).setType(UserType.DEV_XIAOMI)

    // 初始化StreamingContext
    val sparkConf = new SparkConf().setAppName("NetworkWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    // 从监听端口接收数据，然后写入Talos
    val lines = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_AND_DISK_SER)
    val words = lines.foreachRDD { rdd =>
      rdd.foreachPartition { iterator =>
        // 重要：调用单例的Talos Producer
        val talosProducer = TalosProducerSingleton.getProducer(talosProperties, credential, topicName)

        val messageList = new util.ArrayList[Message]()
        iterator.foreach(m => messageList.add(new Message(ByteBuffer.wrap(m.getBytes("UTF-8")))))

        talosProducer.addUserMessage(messageList)
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }

}
