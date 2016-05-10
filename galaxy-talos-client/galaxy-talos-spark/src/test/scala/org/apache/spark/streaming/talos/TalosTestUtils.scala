package org.apache.spark.streaming.talos

import java.nio.ByteBuffer
import java.util
import java.util.{Map => JMap, Set => JSet}

import com.xiaomi.infra.galaxy.rpc.thrift.{Credential, UserType}
import com.xiaomi.infra.galaxy.talos.client.{SimpleTopicAbnormalCallback, TalosClientConfigKeys}
import com.xiaomi.infra.galaxy.talos.producer.{TalosProducer, TalosProducerConfig, UserMessageCallback, UserMessageResult}
import com.xiaomi.infra.galaxy.talos.thrift._
import org.apache.spark.Logging
import org.scalatest.concurrent.Eventually

import scala.collection.mutable.ArrayBuffer
import scala.collection.{immutable, mutable}
import scala.concurrent.duration._
import scala.collection.JavaConverters._

/**
  * Created by jiasheng on 16-3-21.
  */
private[talos]
class TalosTestUtils(params: immutable.Map[String, String])
  extends Logging
    with Eventually {

  // for java
  def this(params: JMap[String, String]) = {
    this(immutable.Map(params.asScala.toSeq: _*))
  }

  private val uri = ""
  // credential key
  private val key = ""
  // credential secret
  private val secret = ""
  val talosParams = immutable.Map[String, String](
    TalosClientConfigKeys.GALAXY_TALOS_SECURE_SERVICE_ENDPOINT -> uri
  ) ++ params
  val javaTalosParams = new util.HashMap[String, String]()
  talosParams.foreach { case (k, v) => javaTalosParams.put(k, v) }

  val credential = new Credential()
    .setSecretKeyId(key).setSecretKey(secret).setType(UserType.DEV_XIAOMI)
  val tc = new TalosCluster(talosParams, credential)
  private val _producers = mutable.Map.empty[String, TalosProducer]

  def createTopic(topic: String, partitionNum: Int): Unit = {
    val attr = new TopicAttribute().setPartitionNumber(partitionNum)
    val request = new CreateTopicRequest()
      .setTopicName(topic)
      .setTopicAttribute(attr)
    try {
      tc.admin().createTopic(request)
    } catch {
      case ge: GalaxyTalosException
        if ge.errorCode == ErrorCode.TOPIC_EXIST => // ignore
      case e: Exception => throw e
    }
  }

  def deleteTopic(topic: String): Unit = {
    try {
      val topicInfo = tc.admin().describeTopic(new DescribeTopicRequest(topic))
      tc.admin().deleteTopic(new DeleteTopicRequest(topicInfo.topicInfo.topicTalosResourceName))
    } catch {
      case e: Exception => logWarning(s"Delete topic ${topic} failed! " + e.toString)
    }
  }

  def producer(topic: String): TalosProducer = {
    if (!_producers.contains(topic)) {
      val topicInfo = tc.admin().describeTopic(new DescribeTopicRequest(topic))
      _producers += topic -> new TalosProducer(new TalosProducerConfig(tc.config),
        credential, topicInfo.topicInfo.topicTalosResourceName,
        new SimpleTopicAbnormalCallback(), new RetryMessageCallback(topic))
    }
    _producers(topic)
  }

  def sendMessages(topic: String, message: String*): Unit = {
    val talosMessages = message.map(m => new Message(ByteBuffer.wrap(m.getBytes))).toList
    import scala.collection.JavaConverters._
    producer(topic).addUserMessage(talosMessages.asJava)
  }

  def sendMessagesAndWaitForReceive(topic: String, message: String*): Unit = {
    sendMessages(topic, message: _*)
    val partitions = tc.admin().describeTopic(new DescribeTopicRequest(topic))
      .topicAttribute.partitionNumber
    eventually(timeout(10 seconds), interval(50 milliseconds)) {
      val buffer = new ArrayBuffer[String]()
      (0 until partitions).foreach { i =>
        val resp = tc.simpleConsumer(topic, i).fetchMessage(0, message.size)
        import scala.collection.JavaConverters._
        resp.asScala.foreach(mo => buffer += new String(mo.message.message.array()))
      }
      assert(message.forall(buffer.contains))
    }
  }

  def sendMessagesAndWaitForReceive(topic: String, messages: JSet[String]): Unit = {
    sendMessagesAndWaitForReceive(topic, messages.asScala.toSeq: _*)
  }


  private class RetryMessageCallback(topic: String) extends UserMessageCallback {
    @Override def onSuccess(userMessageResult: UserMessageResult) {
    }

    @Override def onError(userMessageResult: UserMessageResult) {
      try {
        TalosTestUtils.this.producer(topic).addUserMessage(userMessageResult.getMessageList)
      }
      catch {
        case e: Exception =>
      }
    }
  }

}

