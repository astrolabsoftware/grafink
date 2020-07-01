package com.astrolabsoftware.grafink.reader

import java.time.LocalDate

import zio._
import zio.test._
import zio.test.Assertion._

import com.astrolabsoftware.grafink.common.PartitionManager
import com.astrolabsoftware.grafink.common.PartitionManager.dateFormat
import com.astrolabsoftware.grafink.logging.Logger
import com.astrolabsoftware.grafink.models.{ Parquet, ReaderConfig }
import com.astrolabsoftware.grafink.services.reader.Reader
import com.astrolabsoftware.grafink.utils.SparkTestEnv

object ReaderSpec extends DefaultRunnableSpec {

  val sparkLayer = SparkTestEnv.test

  def spec: ZSpec[Environment, Failure] = suite("ReaderSpec")(
    testM("Reader will correctly read values from valid path") {
      val dateString = "2019-02-01"
      val date       = LocalDate.parse(dateString, dateFormat)
      val dataPath   = "/data"
      val path       = getClass.getResource(dataPath).getPath
      val logger     = Logger.test
      val config     = ZLayer.succeed(ReaderConfig(path, Parquet, keepCols = List("objectId"), keepColsRenamed = List()))

      val app =
        for {
          df <- Reader.read(PartitionManager(date, 1))
        } yield df.count

      val layer = ((logger ++ config ++ sparkLayer) >>> Reader.live) ++ logger
      assertM(app.provideLayer(layer))(equalTo(5L))
    }
  )
}
