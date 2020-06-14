package com.astrolabsoftware.grafink.services

import java.time.LocalDate

import zio.ZLayer
import zio.test._
import zio.test.Assertion._

import com.astrolabsoftware.grafink.Job.JobTime
import com.astrolabsoftware.grafink.common.PartitionManager.dateFormat
import com.astrolabsoftware.grafink.logging.Logger
import com.astrolabsoftware.grafink.models.{ HBaseColumnConfig, IDManagerConfig, SparkPathConfig }
import com.astrolabsoftware.grafink.utils.SparkTestEnv

object IDManagerSparkServiceSpec extends DefaultRunnableSpec {

  def spec: ZSpec[Environment, Failure] = suite("IDManagerSparkServiceSpec")(
    testM("IDManagerSparkService will correctly return -1 if no data exists") {
      val dateString    = "2019-02-03"
      val date          = LocalDate.parse(dateString, dateFormat)
      val dataPath      = "/iddata"
      val path          = getClass.getResource(dataPath).getPath
      val logger        = Logger.test
      val jobTime       = JobTime(date, 1)
      val app           = IDManager.fetchID(jobTime)
      val idConfigLayer = ZLayer.succeed(IDManagerConfig(SparkPathConfig(path), HBaseColumnConfig("", "", "")))

      val sparkLayer            = SparkTestEnv.test
      val idManagerServiceLayer = (logger ++ sparkLayer ++ idConfigLayer) >>> IDManager.liveUSpark

      assertM(app.provideLayer(Logger.live ++ idManagerServiceLayer))(equalTo(0L))
    }
  )
}
