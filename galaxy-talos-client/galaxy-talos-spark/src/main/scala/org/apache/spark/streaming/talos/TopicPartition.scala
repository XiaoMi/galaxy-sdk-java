package org.apache.spark.streaming.talos

/**
  * Created by jiasheng on 16-3-20.
  */
private[talos]
case class TopicPartition(topic: String, partition: Int) {
  def this(tuple: (String, Int)) = this(tuple._1, tuple._2)

  def asTuple: (String, Int) = (topic, partition)

  override def toString: String = "[%s,%d]".format(topic, partition)
}
