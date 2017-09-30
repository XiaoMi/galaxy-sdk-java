package org.apache.spark.streaming.talos.perfcounter

import java.util.concurrent.CopyOnWriteArrayList

import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.SparkConf

/**
 * Created by jiasheng on 15/12/2016.
 */
private[talos] abstract class PerfListener(
  sparkConf: SparkConf
) {
  protected val endpoint: String = "streaming.monitor"
  protected val appName = sparkConf.get("spark.app.name")
  protected val user = UserGroupInformation.getCurrentUser.getShortUserName
  protected val stepInSeconds = sparkConf.getInt("spark.metrics.push.interval.secs", 60)
  protected val clusterName = sparkConf.get("spark.metrics.cluster.name", "unknown")
  protected val perfType = sparkConf.get("spark.metrics.type", "tst")

  def onGeneratePerf(): Seq[PerfBean]

}

private[perfcounter] object PerfListenerBus {
  private val listeners = new CopyOnWriteArrayList[PerfListener]

  import scala.collection.JavaConverters._

  def iterator(): Iterator[PerfListener] = listeners.iterator().asScala

  def addListener(listener: PerfListener): Unit = {
    listeners.add(listener)
  }

  def removeListener(listener: PerfListener): Unit = {
    listeners.remove(listener)
  }
}
