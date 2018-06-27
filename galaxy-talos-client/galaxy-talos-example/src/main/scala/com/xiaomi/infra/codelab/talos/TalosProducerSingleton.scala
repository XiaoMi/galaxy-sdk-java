package com.xiaomi.infra.codelab.talos

import java.util.Properties

import org.slf4j.LoggerFactory

import com.xiaomi.infra.galaxy.rpc.thrift.Credential
import com.xiaomi.infra.galaxy.talos.admin.TalosAdmin
import com.xiaomi.infra.galaxy.talos.client.{SimpleTopicAbnormalCallback, TalosClientConfig, TopicAbnormalCallback}
import com.xiaomi.infra.galaxy.talos.producer.{TalosProducer, TalosProducerConfig, UserMessageCallback, UserMessageResult}
import com.xiaomi.infra.galaxy.talos.thrift.{DescribeTopicRequest, Topic, TopicTalosResourceName}

object TalosProducerSingleton {
  private val LOG = LoggerFactory.getLogger(TalosProducerSingleton.getClass)

  private val talosResourceName = scala.collection.mutable.Map.empty[String, TopicTalosResourceName]
  private var talosAdmin: TalosAdmin = null
  private var talosProducer: TalosProducer = null

  def getProducer(
      properties: Properties,
      credential: Credential,
      topicName: String
  ): Unit = this.synchronized {
    getProducer(properties, credential, topicName, new SimpleTopicAbnormalCallback(), new RetryMessageCallback())
  }

  def getProducer(
      properties: Properties,
      credential: Credential,
      topicName: String,
      topicAbnormalCallback: TopicAbnormalCallback,
      userMessageCallback: UserMessageCallback
  ): Unit = this.synchronized {
    if (talosProducer == null) {
      val talosAdmin = getTalosAdmin(properties, credential)
      val topicResourceName = getTalosResourceName(talosAdmin, topicName)
      talosProducer = new TalosProducer(new TalosProducerConfig(properties), credential, topicResourceName, topicAbnormalCallback, userMessageCallback)
    }
    talosProducer
  }

  private def getTalosAdmin(properties: Properties, credential: Credential): TalosAdmin = {
    if (talosAdmin == null) {
      talosAdmin = new TalosAdmin(new TalosClientConfig(properties), credential)
    }
    talosAdmin
  }

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

    talosResourceName.getOrElseUpdate(topicName, topic.getTopicInfo.getTopicTalosResourceName)
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
