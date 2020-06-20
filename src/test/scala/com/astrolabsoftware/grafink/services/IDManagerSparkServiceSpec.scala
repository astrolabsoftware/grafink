package com.astrolabsoftware.grafink.services

import java.time.LocalDate

import zio.{ ZIO, ZLayer }
import zio.test._
import zio.test.Assertion._
import zio.test.environment.TestConsole

import com.astrolabsoftware.grafink.Job.{ JobTime, SparkEnv }
import com.astrolabsoftware.grafink.common.PartitionManager
import com.astrolabsoftware.grafink.common.PartitionManager.dateFormat
import com.astrolabsoftware.grafink.logging.Logger
import com.astrolabsoftware.grafink.models.{
  HBaseColumnConfig,
  IDManagerConfig,
  Parquet,
  ReaderConfig,
  SparkPathConfig
}
import com.astrolabsoftware.grafink.processor.VertexProcessorSpec.{ getClass, sparkLayer }
import com.astrolabsoftware.grafink.services.reader.Reader
import com.astrolabsoftware.grafink.utils.{ SparkTestEnv, TempDirService }

object IDManagerSparkServiceSpec extends DefaultRunnableSpec {

  def spec: ZSpec[Environment, Failure] = suite("IDManagerSparkServiceSpec")(
    testM("IDManagerSparkService will correctly return 0 if no data exists") {
      val dateString      = "2019-02-03"
      val date            = LocalDate.parse(dateString, dateFormat)
      val dataPath        = "/iddata"
      val currentDataPath = "/data"
      val path            = getClass.getResource(dataPath).getPath
      val logger          = Logger.test
      val jobTime         = JobTime(date, 1)
      val app = for {
        spark <- ZIO.access[SparkEnv](_.get.sparkEnv)
        df    <- ZIO.effect(spark.read.parquet(getClass.getResource(currentDataPath).getPath))
        // All the idmanager data so far ingested
        idManagerDf <- IDManagerSparkService.readAll(df.schema)
        // Get the last max id used
        lastMax <- IDManagerSparkService.fetchID(idManagerDf)
      } yield lastMax

      val idConfigLayer = ZLayer.succeed(IDManagerConfig(SparkPathConfig(path), HBaseColumnConfig("", "", "")))

      val sparkLayer                 = SparkTestEnv.test
      val idManagerSparkServiceLayer = (logger ++ sparkLayer ++ idConfigLayer) >>> IDManagerSparkService.live

      assertM(app.provideLayer(Logger.live ++ sparkLayer ++ idManagerSparkServiceLayer))(equalTo(0L))
    },
    testM("IDManagerSparkService will correctly add ids to input alerts data") {
      val dateString = "2019-02-01"
      val date       = LocalDate.parse(dateString, dateFormat)
      val dataPath   = "/data"
      val path       = getClass.getResource(dataPath).getPath
      val logger     = Logger.test
      val readerConfig =
        ZLayer.succeed(ReaderConfig(path, Parquet, keepCols = List("objectId"), keepColsRenamed = List()))

      val tempDirServiceLayer = (zio.console.Console.live) >>> TempDirService.test
      val runtime             = zio.Runtime.default
      val tempDir =
        runtime.unsafeRun(TempDirService.createTempDir.provideLayer(tempDirServiceLayer ++ zio.console.Console.live))

      val idManagerConfig =
        ZLayer.succeed(IDManagerConfig(SparkPathConfig(tempDir.getAbsolutePath), HBaseColumnConfig("", "", "")))

      val app = for {
        df         <- Reader.read(PartitionManager(date, 1))
        vertexData <- IDManagerSparkService.process(df)
        idData     <- ZIO.effect(vertexData.current.select("id").collect)
        _          <- TempDirService.removeTempDir(tempDir)
        ids = idData.map(_.getLong(0))
      } yield ids.toIterable

      val readerLayer = (logger ++ readerConfig ++ sparkLayer) >>> Reader.live
      val idManagerSparkServiceLayer =
        (logger ++ sparkLayer ++ ((tempDirServiceLayer ++ TestConsole.debug) >>> idManagerConfig)) >>> IDManagerSparkService.live

      val layer =
        tempDirServiceLayer ++ TestConsole.debug ++ idManagerSparkServiceLayer ++ logger ++ readerLayer ++ sparkLayer

      assertM(app.provideLayer(layer))(hasSameElementsDistinct(Array[Long](1, 2, 3, 4, 5)))
    }
  )
}
