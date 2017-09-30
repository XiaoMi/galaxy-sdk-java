package org.apache.spark.streaming.talos

import org.scalatest.FunSuite

class DirectStreamSuite extends FunSuite {
  test("topic recreated.") {
    val currentOffsets = Map(
      topicPartition(0) -> 100L,
      topicPartition(1) -> 200L
    )
    val latestOffsets = Map(
      topicPartition(0) -> 50L,
      topicPartition(1) -> 100L
    )

    val fromOffsets = DirectTalosInputDStream.reviseCurrentOffsets(currentOffsets, latestOffsets)
    assert(fromOffsets === Map(topicPartition(0) -> 0L, topicPartition(1) -> 0L))
  }

  test("topic partition number shrink") {
    val currentOffsets = Map(
      topicPartition(0) -> 100L,
      topicPartition(1) -> 200L
    )
    val latestOffsets = Map(
      topicPartition(0) -> 50L
    )

    val fromOffsets = DirectTalosInputDStream.reviseCurrentOffsets(currentOffsets, latestOffsets)
    assert(fromOffsets === Map(topicPartition(0) -> 0L))
  }

  test("topic partition number enlarged") {
    val currentOffsets = Map(
      topicPartition(0) -> 100L
    )
    val latestOffsets = Map(
      topicPartition(0) -> 200L,
      topicPartition(1) -> 100L
    )

    val fromOffsets = DirectTalosInputDStream.reviseCurrentOffsets(currentOffsets, latestOffsets)
    assert(fromOffsets === Map(topicPartition(0) -> 100L, topicPartition(1) -> 0L))
  }

  private def topicPartition(partition: Int): TopicPartition = {
    TopicPartition("test-topic", partition)
  }

}
