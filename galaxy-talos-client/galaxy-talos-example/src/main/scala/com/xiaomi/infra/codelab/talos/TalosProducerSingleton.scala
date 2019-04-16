package com.xiaomi.infra.codelab.talos

import java.util.Properties

import scala.collection.mutable

import org.slf4j.LoggerFactory

import com.xiaomi.infra.galaxy.rpc.thrift.Credential
import com.xiaomi.infra.galaxy.talos.admin.TalosAdmin
import com.xiaomi.infra.galaxy.talos.client.{SimpleTopicAbnormalCallback, TalosClientConfig}
import com.xiaomi.infra.galaxy.talos.producer.{TalosProducer, TalosProducerConfig, UserMessageCallback, UserMessageResult}
import com.xiaomi.infra.galaxy.talos.thrift.{DescribeTopicRequest, Topic, TopicTalosResourceName}

class TalosProducerSingleton(
    properties: Properties,
    credential: Credential,
    topicName: String) extends Serializable {

  import TalosProducerSingleton._

  @transient private var talosProducer: TalosProducer = null

  /**
   * 获取单例的 Talos Producer
   *
   * @return
   */
  def getProducer: TalosProducer = this.synchronized {
    if (talosProducer == null) {
      val talosAdmin = getTalosAdmin(properties, credential)
      val topicResourceName = getTalosResourceName(talosAdmin, topicName)
      talosProducer = new TalosProducer(
        new TalosProducerConfig(properties),
        credential,
        topicResourceName,
        new SimpleTopicAbnormalCallback(),
        new RetryMessageCallback())
    }
    talosProducer
  }

  private def getTalosAdmin(properties: Properties, credential: Credential): TalosAdmin = {
    new TalosAdmin(new TalosClientConfig(properties), credential)
  }

  /**
   * 获取 TopicTalosResourceName, 如果获取过程连续失败3次，则抛出异常；
   *
   * @param talosAdmin
   * @param topicName
   * @return
   */
  private def getTalosResourceName(talosAdmin: TalosAdmin, topicName: String): TopicTalosResourceName = {
    val maxRetry = 3
    var topic: Topic = null

    // Retry in case of failure caused by QPS throttle.
    for (i <- 1 to maxRetry if topic == null) {
      try {
        topic = talosAdmin.describeTopic(new DescribeTopicRequest(topicName))
      } catch {
        case t: Throwable =>
          if (i == maxRetry) {
            throw t
          }
          LOG.warn(s"Describe topic failed, retry $i seconds later.", t)
          Thread.sleep(i * 1000)
      }
    }
    topic.getTopicInfo.getTopicTalosResourceName
  }

  // Simple UserMessageCallback which retries to put messages in case of failure.
  private class RetryMessageCallback extends UserMessageCallback {
    def onSuccess(userMessageResult: UserMessageResult) {
    }

    def onError(userMessageResult: UserMessageResult) {
      try {
        LOG.warn(s"Failed to put ${userMessageResult.getMessageList.size()} messages, we will retry to put it.")
        talosProducer.addUserMessage(userMessageResult.getMessageList)
      } catch {
        case t: Throwable => {
          LOG.error("Retry putting messages failed.", t)
        }
      }
    }
  }

}

object TalosProducerSingleton {
  private val LOG = LoggerFactory.getLogger(TalosProducerSingleton.getClass)
  @transient private val producers = mutable.Map.empty[String, TalosProducerSingleton]

  /**
   * 获取Talos Producer单例；使用默认的 TopicAbnormalCallback 和 UserMessageCallback ；
   *
   * @param properties 包含了talos相关的配置，比如'galaxy.talos.service.endpoint'；
   * @param credential 认证相关的配置;
   * @param topicName  topic名称;
   */
  def getProducer(
      properties: Properties,
      credential: Credential,
      topicName: String
  ): TalosProducer = this.synchronized {
    producers.getOrElseUpdate(topicName, new TalosProducerSingleton(properties, credential, topicName)).getProducer
  }

}
