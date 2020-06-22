package com.astrolabsoftware.grafink.processor

import java.time.LocalDate

import scala.collection.JavaConverters._

import zio.{ ZIO, ZLayer }
import zio.test.{ DefaultRunnableSpec, _ }
import zio.test.Assertion._
import zio.test.environment.TestConsole

import com.astrolabsoftware.grafink.Job.JobTime
import com.astrolabsoftware.grafink.common.PartitionManager
import com.astrolabsoftware.grafink.common.PartitionManager.dateFormat
import com.astrolabsoftware.grafink.logging.Logger
import com.astrolabsoftware.grafink.models._
import com.astrolabsoftware.grafink.services.IDManagerSparkService
import com.astrolabsoftware.grafink.services.reader.Reader
import com.astrolabsoftware.grafink.utils.{ JanusGraphTestEnv, SparkTestEnv, TempDirService }

object VertexProcessorSpec extends DefaultRunnableSpec {

  val sparkLayer = SparkTestEnv.test

  def spec: ZSpec[Environment, Failure] = suite("VertexProcessorSpec")(
    testM("VertexProcessor will correctly add input alerts into janusgraph") {
      val dateString = "2019-02-01"
      val date       = LocalDate.parse(dateString, dateFormat)
      val dataPath   = "/data"
      val path       = getClass.getResource(dataPath).getPath
      val logger     = Logger.test
      val readerConfig =
        ZLayer.succeed(ReaderConfig(path, Parquet, keepCols = List("objectId"), keepColsRenamed = List()))
      val janusConfig =
        JanusGraphConfig(
          SchemaConfig(vertexPropertyCols = List(""), vertexLabel = "", edgeLabels = List()),
          VertexLoaderConfig(10),
          EdgeLoaderConfig(10, 1, EdgeRulesConfig(SimilarityConfig(""))),
          JanusGraphStorageConfig("", 0, tableName = "test")
        )

      val jobTime = JobTime(date, 1)

      val tempDirServiceLayer = ((zio.console.Console.live) >>> TempDirService.test)
      val runtime             = zio.Runtime.default
      val tempDir =
        runtime.unsafeRun(TempDirService.createTempDir.provideLayer(tempDirServiceLayer ++ zio.console.Console.live))

      val idManagerConfig =
        ZLayer.succeed(IDManagerConfig(SparkPathConfig(tempDir.getAbsolutePath), HBaseColumnConfig("", "", "")))

      val app = for {
        spark <- SparkTestEnv.sparkEnv
        output <- JanusGraphTestEnv
          .test(janusConfig)
          .use(graph =>
            for {
              df <- Reader.read(PartitionManager(date, 1))
              _  <- VertexProcessor.process(jobTime, df)
              g = graph.traversal()
            } yield g.V().toList.asScala.map(_.property("objectId").value().toString).toList
          )
      } yield (output)

      val readerLayer = (logger ++ readerConfig ++ sparkLayer) >>> Reader.live
      val idManagerLayer =
        (logger ++ sparkLayer ++ ((tempDirServiceLayer ++ TestConsole.debug) >>> idManagerConfig)) >>> IDManagerSparkService.live
      val vertexProcessorLayer = (sparkLayer ++ ZLayer.succeed(janusConfig) ++ logger) >>> VertexProcessor.live
      val layer =
        tempDirServiceLayer ++ TestConsole.debug ++ vertexProcessorLayer ++ idManagerLayer ++ logger ++ readerLayer ++ sparkLayer

      assertM(app.provideLayer(layer))(
        hasSameElementsDistinct(List("ZTF19acmcetc", "ZTF17aaanypg", "ZTF19acmbxka", "ZTF19acmbxfe", "ZTF19acmbtac"))
      )
    } @@ TestAspect.ignore
  )
}
