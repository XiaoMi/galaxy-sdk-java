package org.apache.spark.streaming.talos.perfcounter

import scala.collection.mutable

import org.apache.spark.streaming.dstream.DStream

private[talos] class ActiveRDDsPerfListener(
  dstream: DStream[_],
  topics: Set[String]
) extends PerfListener(dstream.context.sparkContext.conf) {

  override def onGeneratePerf(): Seq[PerfBean] = {
    Seq(PerfBean(
      endpoint,
      "TalosRDDLag",
      System.currentTimeMillis(),
      stepInSeconds,
      dstream.generatedRDDs.size,
      CounterType.GAUGE,
      mutable.LinkedHashMap(
        "appName" -> appName,
        "user" -> user,
        "cluster" -> clusterName,
        "type" -> perfType,
        "topics" -> topics.mkString("&")
      )
    ))
  }
}
