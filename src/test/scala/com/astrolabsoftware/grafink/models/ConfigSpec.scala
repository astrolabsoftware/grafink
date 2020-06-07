package com.astrolabsoftware.grafink.models

import zio.test._
import zio.test.Assertion._

import com.astrolabsoftware.grafink.logging.Logger
import com.astrolabsoftware.grafink.models.config.{ Config, GrafinkConfig }

object ConfigSpec extends DefaultRunnableSpec {
  def spec: ZSpec[Environment, Failure] =
    suite("ConfigSpec")(
      testM("ReaderConfig is parsed correctly") {
        val path = getClass.getResource("/application.conf").getPath

        val layer = Logger.test >>> Config.live(path)

        val cfg = Config.readerConfig
        assertM(cfg.provideLayer(layer))(equalTo(ReaderConfig(basePath = "/test/base/path", format = Parquet)))
      },
      testM("HBaseConfig is parsed correctly") {

        val path = getClass.getResource("/application.conf").getPath

        val layer = Logger.test >>> Config.live(path)

        val cfg = Config.hbaseConfig
        assertM(cfg.provideLayer(layer))(equalTo(HBaseConfig(HBaseZookeeperConfig(quoram = "localhost"))))
      },
      testM("Invalid config file throws") {
        val path  = getClass.getResource("/invalidapplication.conf").getPath
        val layer = Logger.test >>> Config.live(path)
        val cfg   = Config.readerConfig
        assertM(cfg.provideLayer(layer).run)(
          fails(isSubtype[pureconfig.error.ConfigReaderException[GrafinkConfig]](anything))
        )
      }
    )
}
