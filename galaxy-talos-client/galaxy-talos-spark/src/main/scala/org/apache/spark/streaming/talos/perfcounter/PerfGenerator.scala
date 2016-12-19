package org.apache.spark.streaming.talos.perfcounter

import org.apache.spark.Logging

import scala.collection.mutable

/**
  * Created by jiasheng on 14/12/2016.
  */
private[perfcounter] object PerfGenerator extends Logging {
  def getPerfString(): String = {
    val iter = PerfListenerBus.iterator()
    val seq = mutable.Buffer.empty[PerfBean]
    while (iter.hasNext) {
      val listener = iter.next()
      try {
        seq ++= listener.onGeneratePerf()
      } catch {
        case e: Exception => logWarning(
          s"Calling onGeneratePerf() of ${listener.getClass.getSimpleName} failed!", e)
      }
    }
    if (seq.isEmpty) {
      ""
    } else {
      s"[${seq.map(_.toJsonStr).mkString(",")}]"
    }
  }
}
