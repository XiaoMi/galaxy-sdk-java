package org.apache.spark.streaming.talos.perfcounter

import java.util.concurrent.{Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.spark.{Logging, SparkConf}

/**
  * Created by jiasheng on 14/12/2016.
  */
object PerfReporter extends Logging {
  private final val DEFAULT_PERF_URL = "http://127.0.0.1:1988/v1/push"

  private lazy val executorService = Executors.newSingleThreadScheduledExecutor()
  private lazy val httpClient = new DefaultHttpClient()
  private var httpPost: HttpPost = null

  private val started = new AtomicBoolean(false)
  private var pushIntervalSec: Long = 60

  def register(listener: PerfListener): Unit = {
    PerfListenerBus.addListener(listener)
  }

  def unregister(listener: PerfListener): Unit = {
    PerfListenerBus.removeListener(listener)
  }

  private def init(conf: SparkConf): Unit = {
    if (httpPost == null) {
      val perfUrl = conf.get("spark.metrics.push.url", DEFAULT_PERF_URL)
      httpPost = new HttpPost(perfUrl)
    }
    pushIntervalSec = conf.getLong("spark.metrics.push.interval.secs", pushIntervalSec)
  }

  def isPerfPushEnabled(conf: SparkConf): Boolean = {
    conf.getBoolean("spark.metrics.push.enable", false)
  }

  def startIfEnabled(conf: SparkConf): Unit = {
    if (!isPerfPushEnabled(conf)) {
      return
    }
    if (started.compareAndSet(false, true)) {
      init(conf)
      executorService.scheduleAtFixedRate(
        pushPerf, pushIntervalSec, pushIntervalSec, TimeUnit.SECONDS)
      logInfo("PerfReporter started.")
    } else {
      logWarning("PerfReporter has already started!")
    }
  }

  def isRunning: Boolean = started.get()

  def stop(): Unit = {
    if (started.compareAndSet(true, false)) {
      executorService.shutdownNow()
      logInfo("PerfReporter stopped.")
    }
  }

  private lazy val pushPerf = new Runnable {
    override def run() = {
      try {
        val jsonStr = PerfGenerator.getPerfString()
        logDebug(s"PerfString: $jsonStr")
        httpPost.setEntity(new StringEntity(jsonStr))
        httpClient.execute(httpPost)
      } catch {
        case e: Exception => logWarning("Push perfString to PerfCounter failed!", e)
      } finally {
        httpPost.releaseConnection()
      }
    }
  }

}
