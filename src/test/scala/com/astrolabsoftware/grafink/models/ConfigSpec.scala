package com.astrolabsoftware.grafink.models

import zio.config._
import zio.test._
import zio.test.Assertion._

import com.astrolabsoftware.grafink.Boot

object ConfigSpec extends DefaultRunnableSpec {
  def spec: ZSpec[Environment, Failure] =
    suite("ConfigSpec")(
      testM("ReaderConfig is parsed correctly") {
        val path = getClass.getResource("/application.conf").getPath

        val layer = Boot.getConfig[ReaderConfig](path, Boot.readerConfigDescription)

        val cfg = config[ReaderConfig]
        assertM(cfg.provideLayer(layer))(equalTo(ReaderConfig(basePath = "/test/base/path")))
      } @@ TestAspect.ignore
    )
}
