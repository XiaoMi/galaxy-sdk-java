package org.apache.spark.streaming.talos

import org.apache.spark.{Logging, SparkFunSuite}
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}

import com.xiaomi.infra.galaxy.auth.authentication.XiaomiAuthSpi
import com.xiaomi.infra.galaxy.talos.common.config.{TalosConfigKeys, TalosConfigurationLoader}
import com.xiaomi.infra.galaxy.talos.server.cluster.TalosMiniCluster

/**
 * Created by jiasheng on 16-3-25.
 */
class TalosClusterSuite extends SparkFunSuite
    with BeforeAndAfter
    with BeforeAndAfterAll
    with Eventually
    with Logging {
  protected var uri: String = _
  private var mockCluster: TalosMiniCluster = _

  override protected def beforeAll(): Unit = {
    setUpCluster()
    uri = mockCluster.getServiceURIList.get(0)
  }

  override protected def afterAll(): Unit = {
    if (mockCluster != null) {
      tearDownCluster()
    }
    uri = null
  }

  private def setUpCluster(): Unit = {
    val configuration = TalosConfigurationLoader.getConfiguration()
    configuration.setInt(TalosConfigKeys.GALAXY_TALOS_TOPIC_META_CACHE_UPDATE_INTERVAL_MILLIS, 3000)

    /**
     * set mock dev login for testing authentication
     */
    configuration.setBoolean(TalosConfigKeys.GALAXY_TALOS_MOCK_DEV_LOGIN, true)
    mockCluster = new TalosMiniCluster(configuration)
    // set client configuration
    // use SimpleAuthProvider as we can't connect to AuthService,
    // which set secretKey and secretKeyId the same value;
    System.setProperty(XiaomiAuthSpi.GALAXY_FDS_XIAOMI_AUTH_SERVICE_PROVIDER_KEY,
      "com.xiaomi.infra.galaxy.auth.authentication.miauth.SimpleAuthProvider")
    mockCluster.start()
  }

  private def tearDownCluster(): Unit = {
    mockCluster.stop()
    mockCluster.clean()
  }
}
