package com.astrolabsoftware.grafink.services

import java.time.LocalDate

import zio.{ ULayer, ZLayer }
import zio.test._
import zio.test.Assertion._
import zio.test.DefaultRunnableSpec
import zio.test.mock.Expectation.value

import com.astrolabsoftware.grafink.Job.JobTime
import com.astrolabsoftware.grafink.common.PartitionManager.dateFormat
import com.astrolabsoftware.grafink.hbase.HBaseClientService.HBaseClientService
import com.astrolabsoftware.grafink.hbase.HBaseClientServiceMock
import com.astrolabsoftware.grafink.logging.Logger
import com.astrolabsoftware.grafink.models.{
  EdgeLoaderConfig,
  EdgeRulesConfig,
  HBaseColumnConfig,
  IDManagerConfig,
  JanusGraphConfig,
  JanusGraphStorageConfig,
  SchemaConfig,
  SimilarityConfig,
  SparkPathConfig,
  VertexLoaderConfig
}

object IDManagerSpec extends DefaultRunnableSpec {

  def spec: ZSpec[Environment, Failure] = suite("IDManagerSpec")(
    testM("IDManagerHBaseService will correctly return id value from hbase") {
      val date                = "2019-02-03"
      val janusGraphTableName = "test"
      val rowKey              = s"$date-$janusGraphTableName"
      val id                  = 1234L
      val jobTime             = JobTime(LocalDate.parse(date, dateFormat), 1)
      val app                 = IDManager.fetchID(jobTime)
      val idConfigLayer       = ZLayer.succeed(IDManagerConfig(SparkPathConfig(""), HBaseColumnConfig("", "", "")))
      val janusConfigLayer =
        ZLayer.succeed(
          JanusGraphConfig(
            SchemaConfig(List(""), "", List()),
            VertexLoaderConfig(10),
            EdgeLoaderConfig(10, 1, 25000, EdgeRulesConfig(SimilarityConfig(""))),
            JanusGraphStorageConfig("", 0, janusGraphTableName)
          )
        )
      val mockEnv: ULayer[HBaseClientService] = (
        HBaseClientServiceMock.GetFromTable(
          equalTo(rowKey),
          value(Some(s"$id"))
        )
      )
      val result =
        app.provideLayer(
          (((mockEnv ++ Logger.test) ++ idConfigLayer ++ janusConfigLayer) >>> IDManager.liveUHbase) ++ Logger.test
        )
      assertM(result)(equalTo(id))
    }
  )
}
