package org.apache.spark.streaming.talos

import org.apache.spark.SparkFunSuite
import org.scalatest.BeforeAndAfterAll
import scala.collection.immutable

/**
  * Created by jiasheng on 16-3-25.
  */
class TalosClusterSuite extends SparkFunSuite with BeforeAndAfterAll {
  private val topic = "spark-talos-cluster-test"
  private var talosTestUtils: TalosTestUtils = _

  override protected def beforeAll(): Unit = {
    talosTestUtils = new TalosTestUtils(immutable.Map[String, String]())
    talosTestUtils.deleteTopic(topic)
    talosTestUtils.createTopic(topic, 1)
    talosTestUtils.sendMessagesAndWaitForReceive(topic, "message", "message")
  }

  override protected def afterAll(): Unit = {}

  test("cache creating instance") {
    val config = talosTestUtils.tc.config
    assert(config.equals(talosTestUtils.tc.config), "Not using cached Configuration instance")
    val admin = talosTestUtils.tc.admin()
    assert(admin.equals(talosTestUtils.tc.admin()), "Not using cached TalosAdmin instance")
    val resourceName = talosTestUtils.tc.topicResourceName(topic)
    assert(resourceName.equals(talosTestUtils.tc.topicResourceName(topic)))
    val simpleConsumer = talosTestUtils.tc.simpleConsumer(topic, 0)
    assert(simpleConsumer.equals(talosTestUtils.tc.simpleConsumer(topic, 0)))
  }

  test("earliest offset api") {
    val offset = talosTestUtils.tc.getEarliestOffsets(Set(topic)).right.get
    assert(offset.head._2 === 0, "didn't get earliest offset")

  }

  test("latest offset api") {
    val offset = talosTestUtils.tc.getLatestOffsets(Set(topic)).right.get
    assert(offset.head._2 === 2, "didn't get latest offset")
  }
}
