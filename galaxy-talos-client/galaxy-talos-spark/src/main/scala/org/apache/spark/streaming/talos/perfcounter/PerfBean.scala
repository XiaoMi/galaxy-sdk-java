package org.apache.spark.streaming.talos.perfcounter

import org.apache.spark.streaming.talos.perfcounter.CounterType.CounterType
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import scala.collection.mutable

/**
  * Created by jiasheng on 19/12/2016.
  */
case class PerfBean(
  endpoint: String,
  metric: String,
  timestamp: Long,
  step: Int,
  value: Long,
  counterType: CounterType,
  tags: mutable.LinkedHashMap[String, String]
) {
  def toJsonStr: String = {
    val json = ("endpoint" -> endpoint) ~
      ("metric" -> metric) ~
      ("timestamp" -> timestamp) ~
      ("step" -> step) ~
      ("value" -> value) ~
      ("counterType" -> counterType.toString) ~
      ("tags" -> tags.map(t => s"${t._1}=${t._2}").mkString(","))
    compact(render(json))
  }
}

object CounterType extends Enumeration {
  type CounterType = Value
  val COUNTER = Value("COUNTER")
  val GAUGE = Value("GAUGE")
}
