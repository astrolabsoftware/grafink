package com.astrolab.software.grafink.api

import zio.test._
import zio.test.Assertion._

import com.astrolabsoftware.grafink.api.apiconfig.APIConfig
import com.astrolabsoftware.grafink.logging.Logger
import com.astrolabsoftware.grafink.models._
import com.astrolabsoftware.grafink.models.config.Config

object APIConfigSpec extends DefaultRunnableSpec {
  def spec: ZSpec[Environment, Failure] =
    suite("ConfigSpec")(
      testM("APIConfig is parsed correctly") {
        val path = getClass.getResource("/application.conf").getPath

        val layer = Logger.test >>> APIConfig.live(path)
        val cfg = for {
          appConfig   <- APIConfig.appConfig
          janusConfig <- Config.janusGraphConfig
        } yield GrafinkApiConfiguration(appConfig, janusConfig)

        assertM(cfg.provideLayer(layer))(
          equalTo(
            GrafinkApiConfiguration(
              app = AppConfig(port = 9073, cacheSize = 2),
              janusgraph = JanusGraphConfig(
                JanusGraphStorageConfig(
                  "127.0.0.1",
                  8182,
                  tableName = "TestJanusGraph",
                  List("zookeeper.recovery.retry=3")
                ),
                JanusGraphIndexBackendConfig("elastic", "elastictest", "127.0.0.1:9200")
              )
            )
          )
        )
      },
      testM("Invalid api config file throws") {
        val path  = "/invalidapiapplication.conf"
        val layer = Logger.test >>> APIConfig.live(path)
        val cfg   = APIConfig.appConfig
        assertM(cfg.provideLayer(layer).run)(
          fails(isSubtype[java.lang.IllegalStateException](anything))
        )
      }
    )
}
