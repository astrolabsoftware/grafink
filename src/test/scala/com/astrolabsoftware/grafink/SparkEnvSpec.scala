package com.astrolabsoftware.grafink

import buildinfo.BuildInfo
import zio.ZIO
import zio.test._
import zio.test.Assertion._

import com.astrolabsoftware.grafink.Job.SparkEnv

object SparkEnvSpec extends DefaultRunnableSpec {

  def spec: ZSpec[Environment, Failure] =
    suite("SparkEnvSpec")(
      testM("SparkEnv correctly makes a spark object") {
        val env     = SparkEnv.local()
        val appName = ZIO.access[SparkEnv](_.get).provideLayer(env).map(x => x.sparkEnv.conf.get("spark.app.name"))
        assertM(appName)(equalTo(BuildInfo.name))
      }
    )
}
