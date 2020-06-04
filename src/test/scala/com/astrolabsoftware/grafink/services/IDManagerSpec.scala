package com.astrolabsoftware.grafink.services

import java.time.LocalDate

import zio.{ ULayer, ZLayer }
import zio.test._
import zio.test.Assertion._
import zio.test.DefaultRunnableSpec
import zio.test.mock.Expectation.value

import com.astrolabsoftware.grafink.CLParser.dateFormat
import com.astrolabsoftware.grafink.Job.JobTime
import com.astrolabsoftware.grafink.hbase.HBaseClientService.HBaseClientService
import com.astrolabsoftware.grafink.hbase.HBaseClientServiceMock
import com.astrolabsoftware.grafink.logging.Logger
import com.astrolabsoftware.grafink.models.IDManagerConfig

object IDManagerSpec extends DefaultRunnableSpec {

  def spec: ZSpec[Environment, Failure] = suite("IDManagerSpec")(
    testM("IDManager will correctly return id value from hbase") {
      val date               = "2019-02-03"
      val idManagerTableName = "test"
      val rowKey             = s"$date-$idManagerTableName"
      val id                 = 1234L
      val jobTime            = JobTime(LocalDate.parse(date, dateFormat), 1)
      val app                = IDManager.fetchID(jobTime)
      val idConfigLayer      = ZLayer.succeed(IDManagerConfig(idManagerTableName, "", ""))
      val mockEnv: ULayer[HBaseClientService] = (
        HBaseClientServiceMock.GetFromTable(
          equalTo(rowKey),
          value(Some(s"$id"))
        )
      )
      val result =
        app.provideLayer((((mockEnv ++ Logger.test) ++ idConfigLayer) >>> IDManager.live) ++ Logger.test)
      assertM(result)(equalTo(Some(1234L)))
    }
  )
}
