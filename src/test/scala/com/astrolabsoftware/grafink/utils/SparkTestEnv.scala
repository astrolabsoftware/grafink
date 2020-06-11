package com.astrolabsoftware.grafink.utils

import buildinfo.BuildInfo
import org.apache.spark.sql.SparkSession
import zio.{ Has, ZIO, ZLayer, ZManaged }
import zio.blocking.Blocking

import com.astrolabsoftware.grafink.Job.SparkEnv
import com.astrolabsoftware.grafink.SparkEnv.{ make, Service }

object SparkTestEnv {

  def appID: String =
    (this.getClass.getName
      + math.floor(math.random * 10e4).toLong.toString)

  private def m: SparkSession =
    SparkSession
      .builder()
      .appName(s"${BuildInfo.name}-test")
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "localhost")
      .config("spark.app.id", appID)
      .getOrCreate()

  def test: ZLayer[Blocking, Throwable, SparkEnv] = make(m)

  def sparkEnv: ZIO[SparkEnv, Throwable, SparkSession] = ZIO.access(_.get.sparkEnv)
}
