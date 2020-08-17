package com.astrolabsoftware.grafink.processor.vertex

import java.time.LocalDate

import scala.collection.JavaConverters._

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.apache.tinkerpop.gremlin.structure.T
import org.janusgraph.graphdb.database.StandardJanusGraph
import zio.{ ZIO, ZLayer }
import zio.test._
import zio.test.Assertion.{ equalTo, hasSameElementsDistinct }
import zio.test.environment.TestConsole

import com.astrolabsoftware.grafink.common.{ PaddedPartitionManager, Utils }
import com.astrolabsoftware.grafink.common.PartitionManager.dateFormat
import com.astrolabsoftware.grafink.logging.Logger
import com.astrolabsoftware.grafink.models._
import com.astrolabsoftware.grafink.services.IDManagerSparkService
import com.astrolabsoftware.grafink.services.IDManagerSparkService.IDManagerSparkService
import com.astrolabsoftware.grafink.services.reader.Reader
import com.astrolabsoftware.grafink.utils.{ JanusGraphTestEnv, SparkTestEnv, TempDirService }

object VertexProcessorSpec extends DefaultRunnableSpec {

  val sparkLayer = SparkTestEnv.test

  def spec: ZSpec[Environment, Failure] = suite("VertexProcessorSpec")(
    testM("VertexProcessor will correctly add input alerts into janusgraph") {
      val dateString    = "2019-02-01"
      val date          = LocalDate.parse(dateString, dateFormat)
      val dataPath      = "/data"
      val path          = getClass.getResource(dataPath).getPath
      val logger        = Logger.test
      val objectIdIndex = "objectIdIndex"
      val readerConfig =
        ZLayer.succeed(
          ReaderConfig(path, Parquet, keepCols = List("rfscore", "snnscore", "objectId"), keepColsRenamed = List())
        )
      val janusConfig =
        JanusGraphConfig(
          JanusGraphStorageConfig("", 0, tableName = "test", List.empty),
          JanusGraphIndexBackendConfig("", "", "")
        )
      val jobConfig = GrafinkJobConfig(
        SchemaConfig(
          vertexLabels = List(VertexLabelConfig("alert", List.empty, List("rfscore", "snnscore", "objectId"))),
          edgeLabels = List(),
          IndexConfig(
            composite = List(CompositeIndex(name = objectIdIndex, properties = List("objectId"))),
            mixed = List.empty,
            edge = List.empty
          )
        ),
        VertexLoaderConfig(10, "alert", ""),
        EdgeLoaderConfig(
          10,
          1,
          25000,
          List.empty,
          EdgeRulesConfig(
            SimilarityConfig(""),
            TwoModeSimilarityConfig(List.empty),
            SameValueSimilarityConfig(List.empty)
          )
        )
      )
      val grafinkJanusGraphConfig = GrafinkJanusGraphConfig(jobConfig, janusConfig)
      val tempDirServiceLayer     = ((zio.console.Console.live) >>> TempDirService.test)
      val runtime                 = zio.Runtime.default
      val tempDir =
        runtime.unsafeRun(TempDirService.createTempDir.provideLayer(tempDirServiceLayer ++ zio.console.Console.live))

      val idManagerConfig =
        ZLayer.succeed(
          IDManagerConfig(IDManagerSparkConfig(0, tempDir.getAbsolutePath, false), HBaseColumnConfig("", "", ""))
        )

      val app = for {
        output <- JanusGraphTestEnv
          .test(janusConfig)
          .use(graph =>
            for {
              df         <- Reader.readAndProcess(PaddedPartitionManager(date, 1))
              idManager  <- ZIO.access[IDManagerSparkService](_.get)
              vertexData <- idManager.process(df, "")
              vertexProcessorLive = VertexProcessorLive(grafinkJanusGraphConfig)
              dataTypeForVertexPropertyCols = vertexProcessorLive
                .getDataTypeForVertexProperties(jobConfig.schema.vertexLabels.head.propertiesFromData, df)
              _ <- vertexProcessorLive
                .job(jobConfig, graph, dataTypeForVertexPropertyCols, vertexData.current.collect.toIterator)
              g = graph.traversal()
              _ <- TempDirService.removeTempDir(tempDir)
            } yield g.V().toList.asScala.map(_.property("objectId").value().toString).toList
          )
      } yield output

      val readerLayer = (logger ++ readerConfig ++ sparkLayer) >>> Reader.live
      val idManagerLayer =
        (logger ++ sparkLayer ++ ((tempDirServiceLayer ++ TestConsole.debug) >>> idManagerConfig)) >>> IDManagerSparkService.live
      val vertexProcessorLayer =
        (sparkLayer ++ ZLayer.succeed(jobConfig) ++ ZLayer.succeed(janusConfig) ++ logger) >>> VertexProcessor.live
      val layer =
        tempDirServiceLayer ++ TestConsole.debug ++ vertexProcessorLayer ++ idManagerLayer ++ logger ++ readerLayer ++ sparkLayer

      assertM(app.provideLayer(layer))(
        hasSameElementsDistinct(List("ZTF19acmcetc", "ZTF17aaanypg", "ZTF19acmbxka", "ZTF19acmbxfe", "ZTF19acmbtac"))
      )
    },
    testM("VertexProcessor will correctly add fixedvertices into janusgraph") {
      val dateString    = "2019-02-01"
      val date          = LocalDate.parse(dateString, dateFormat)
      val dataPath      = "/data"
      val path          = getClass.getResource(dataPath).getPath
      val logger        = Logger.test
      val objectIdIndex = "objectIdIndex"

      val janusConfig =
        JanusGraphConfig(
          JanusGraphStorageConfig("", 0, tableName = "test", List.empty),
          JanusGraphIndexBackendConfig("", "", "")
        )
      val jobConfig = GrafinkJobConfig(
        SchemaConfig(
          vertexLabels = List(VertexLabelConfig("alert", List.empty, List("rfscore", "snnscore", "objectId"))),
          edgeLabels = List(),
          IndexConfig(
            composite = List(CompositeIndex(name = objectIdIndex, properties = List("objectId"))),
            mixed = List.empty,
            edge = List.empty
          )
        ),
        VertexLoaderConfig(10, "alert", "/fixedvertices.csv"),
        EdgeLoaderConfig(
          10,
          1,
          25000,
          List.empty,
          EdgeRulesConfig(
            SimilarityConfig(""),
            TwoModeSimilarityConfig(List.empty),
            SameValueSimilarityConfig(List.empty)
          )
        )
      )
      val grafinkJanusGraphConfig = GrafinkJanusGraphConfig(jobConfig, janusConfig)

      val vertexProcessorLive = VertexProcessorLive(grafinkJanusGraphConfig)

      val app = for {
        output <- JanusGraphTestEnv
          .test(janusConfig)
          .use(graph =>
            for {
              fixedVertices <- FixedVertexDataReader.readFixedVertexData(jobConfig.vertexLoader)
              _             <- vertexProcessorLive.loadFixedVertices(graph, fixedVertices)
              g = graph.traversal()
            } yield g.V().toList.asScala.map(_.property("recipe").value().toString).toList
          )
      } yield output

      val fixedVertexDataReaderLayer = logger >>> FixedVertexDataReader.live
      val vertexProcessorLayer =
        (sparkLayer ++ ZLayer.succeed(jobConfig) ++ ZLayer.succeed(janusConfig) ++ logger) >>> VertexProcessor.live
      val layer = TestConsole.debug ++ vertexProcessorLayer ++ logger ++ sparkLayer ++ fixedVertexDataReaderLayer

      assertM(app.provideLayer(layer))(
        hasSameElementsDistinct(
          List(
            "supernova",
            "microlensing",
            "asteroids",
            "catalog",
            "intrecipe",
            "longrecipe",
            "floatrecipe",
            "doublerecipe",
            "boolrecipe",
            "unknownrecipe"
          )
        )
      )
    },
    testM("VertexProcessor will correctly delete already added input alerts into janusgraph") {
      val dateString = "2019-02-01"
      val date       = LocalDate.parse(dateString, dateFormat)
      val dataPath   = "/data"
      val path       = getClass.getResource(dataPath).getPath
      val logger     = Logger.test

      val janusConfig =
        JanusGraphConfig(
          JanusGraphStorageConfig("", 0, tableName = "test", List.empty),
          JanusGraphIndexBackendConfig("", "", "")
        )

      val jobConfig = GrafinkJobConfig(
        SchemaConfig(
          vertexLabels = List(VertexLabelConfig("alert", List.empty, List("rfscore", "snnscore"))),
          edgeLabels = List(),
          index = IndexConfig(composite = List.empty, mixed = List.empty, edge = List.empty)
        ),
        VertexLoaderConfig(1, "alert", ""),
        EdgeLoaderConfig(
          10,
          1,
          25000,
          List.empty,
          EdgeRulesConfig(
            SimilarityConfig(""),
            TwoModeSimilarityConfig(List.empty),
            SameValueSimilarityConfig(List.empty)
          )
        )
      )
      val grafinkJanusGraphConfig = GrafinkJanusGraphConfig(jobConfig, janusConfig)
      val vertexSchema = StructType(
        fields = Array(
          StructField("id", LongType, false),
          StructField("objectId", StringType, false),
          StructField("rfscore", DoubleType, false)
        )
      )

      val schemaMap = vertexSchema.map(f => f.name -> f.dataType).toMap

      val verticesList = List(
        new GenericRowWithSchema(Array(1L, "objectid1", 0.345), vertexSchema),
        new GenericRowWithSchema(Array(2L, "objectid2", 0.9987), vertexSchema),
        new GenericRowWithSchema(Array(3L, "objectid3", 0.0), vertexSchema)
      )
      // Delete only first 2 vertices
      val verticesToDelete = verticesList.dropRight(1).toIterator

      @inline
      def getVertexProperties(r: Row): Seq[AnyRef] =
        List("objectId", "rfscore").flatMap { property =>
          val dType = Utils.getClassTag(schemaMap(property))
          List(property, r.getAs[dType.type](property))
        }

      def getVertexParams(r: Row, id: java.lang.Long): Seq[AnyRef] = Seq(T.id, id) ++ getVertexProperties(r)

      val app = for {
        output <- JanusGraphTestEnv
          .test(janusConfig)
          .use {
            graph =>
              val idMgr = graph.asInstanceOf[StandardJanusGraph].getIDManager
              val vertexParams = verticesList.map(vertex =>
                getVertexParams(vertex, java.lang.Long.valueOf(idMgr.toVertexId(vertex.getAs[Long]("id"))))
              )
              for {
                _ <- ZIO.effect(vertexParams.foreach(v => graph.addVertex(v: _*)))
                _ <- ZIO.effect(graph.tx.commit())
                vertexProcessorLive = VertexProcessorLive(grafinkJanusGraphConfig)
                _ <- vertexProcessorLive.deleteJob(graph, verticesToDelete)
              } yield graph.traversal().V().count().toList.asScala.toList.head
          }
      } yield output

      val vertexProcessorLayer =
        (sparkLayer ++ ZLayer.succeed(jobConfig) ++ ZLayer.succeed(janusConfig) ++ logger) >>> VertexProcessor.live
      val layer = TestConsole.debug ++ vertexProcessorLayer ++ logger ++ sparkLayer

      assertM(app.provideLayer(layer))(equalTo(java.lang.Long.valueOf(1L)))
    }
  )
}
