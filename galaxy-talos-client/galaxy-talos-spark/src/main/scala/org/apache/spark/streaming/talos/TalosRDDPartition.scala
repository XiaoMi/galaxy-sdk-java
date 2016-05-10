package org.apache.spark.streaming.talos

import org.apache.spark.Partition

/**
 * Created by jiasheng on 16-3-15.
 */
private[talos]
class TalosRDDPartition(
  val index: Int,
  val offsetRange: OffsetRange
) extends Partition {
  def count(): Long = offsetRange.untilOffset - offsetRange.fromOffset
}
