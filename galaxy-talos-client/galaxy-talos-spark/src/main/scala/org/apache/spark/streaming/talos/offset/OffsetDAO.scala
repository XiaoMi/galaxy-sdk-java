package org.apache.spark.streaming.talos.offset

import org.apache.spark.streaming.Time
import org.apache.spark.streaming.talos.TopicPartition

private[talos] trait OffsetDAO {

  def save(time: Time, offsets: Map[TopicPartition, Long])

  def restore(): Option[Map[TopicPartition, Long]]

}
