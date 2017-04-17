package org.apache.spark.streaming.talos.perfcounter

import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable

private[talos] class ActiveRDDsPerfListener(
  dstream: DStream[_],
  topics: Seq[String]
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
        "topics" -> topics.mkString(",")
      )
    ))
  }
}
