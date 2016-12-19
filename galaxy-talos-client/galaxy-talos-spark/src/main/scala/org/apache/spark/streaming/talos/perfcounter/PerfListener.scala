package org.apache.spark.streaming.talos.perfcounter

import java.util.concurrent.CopyOnWriteArrayList

/**
  * Created by jiasheng on 15/12/2016.
  */
trait PerfListener {

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
