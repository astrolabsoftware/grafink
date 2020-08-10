package com.astrolabsoftware.grafink.processor

import java.time.LocalDate

import scala.collection.JavaConverters._

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{ IntegerType, LongType, StructField, StructType }
import org.apache.tinkerpop.gremlin.structure.Direction
import zio.{ ZIO, ZLayer }
import zio.test.{ DefaultRunnableSpec, _ }
import zio.test.Assertion._
import zio.test.environment.TestConsole

import com.astrolabsoftware.grafink.common.PaddedPartitionManager
import com.astrolabsoftware.grafink.common.PartitionManager.dateFormat
import com.astrolabsoftware.grafink.logging.Logger
import com.astrolabsoftware.grafink.models._
import com.astrolabsoftware.grafink.processor.EdgeProcessor.EdgeColumns._
import com.astrolabsoftware.grafink.processor.vertex.{ VertexProcessor, VertexProcessorLive }
import com.astrolabsoftware.grafink.services.IDManagerSparkService
import com.astrolabsoftware.grafink.services.IDManagerSparkService.IDManagerSparkService
import com.astrolabsoftware.grafink.services.reader.Reader
import com.astrolabsoftware.grafink.utils.{ JanusGraphTestEnv, SparkTestEnv, TempDirService }

object EdgeProcessorSpec extends DefaultRunnableSpec {

  val sparkLayer = SparkTestEnv.test

  def spec: ZSpec[Environment, Failure] = suite("VertexProcessorSpec")(
    test("EdgeProcessor will correctly calculate parallelism for edge data") {
      val edgeLabel         = "similarity"
      val taskSize          = 2500
      val parallelismConfig = 10
      val similarityConfig  = SimilarityConfig("(rfscore AND snnscore) OR objectId")
      val janusConfig =
        JanusGraphConfig(
          JanusGraphStorageConfig("", 0, tableName = "test", List.empty),
          JanusGraphIndexBackendConfig("", "", "")
        )
      val jobConfig = GrafinkJobConfig(
        SchemaConfig(
          vertexLabels = List(VertexLabelConfig("alert", List.empty, List("rfscore", "snnscore", "objectId"))),
          edgeLabels = List(EdgeLabelConfig(name = edgeLabel, List(PropertySchema(name = "value", typ = "long")))),
          index = IndexConfig(composite = List.empty, mixed = List.empty, edge = List.empty)
        ),
        VertexLoaderConfig(10, "alert", ""),
        EdgeLoaderConfig(
          10,
          parallelismConfig,
          taskSize,
          List.empty,
          EdgeRulesConfig(similarityConfig, TwoModeSimilarityConfig(List.empty))
        )
      )
      val grafinkJanusGraphConfig = GrafinkJanusGraphConfig(jobConfig, janusConfig)
      val parallelism1            = EdgeProcessorLive(grafinkJanusGraphConfig).getParallelism(3000).partitions
      val parallelism2            = EdgeProcessorLive(grafinkJanusGraphConfig).getParallelism(300000).partitions
      assert(parallelism1)(equalTo(parallelismConfig)) && assert(parallelism2)(equalTo(121))
    },
    testM("EdgeProcessor will correctly add edges into janusgraph") {
      val dateString       = "2019-02-01"
      val date             = LocalDate.parse(dateString, dateFormat)
      val dataPath         = "/data"
      val path             = getClass.getResource(dataPath).getPath
      val logger           = Logger.test
      val edgeLabel        = "similarity"
      val edgePropertyKey  = "value"
      val similarityConfig = SimilarityConfig("(rfscore AND snnscore) OR objectId")

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
          edgeLabels = List(EdgeLabelConfig(name = edgeLabel, List(PropertySchema(name = "value", typ = "long")))),
          index = IndexConfig(
            composite = List.empty,
            mixed = List.empty,
            edge = List(EdgeIndex(name = "similarityIndex", properties = List("value"), label = edgeLabel))
          )
        ),
        VertexLoaderConfig(10, "alert", ""),
        EdgeLoaderConfig(
          10,
          1,
          25000,
          List.empty,
          EdgeRulesConfig(similarityConfig, TwoModeSimilarityConfig(List.empty))
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

      val edgeSchema = StructType(
        fields = Array(
          StructField(SRCVERTEXFIELD, LongType, false),
          StructField(DSTVERTEXFIELD, LongType, false),
          StructField(PROPERTYVALFIELD, IntegerType, false)
        )
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
              edgeProcessorLive   = EdgeProcessorLive(grafinkJanusGraphConfig)
              dataTypeForVertexPropertyCols = vertexProcessorLive
                .getDataTypeForVertexProperties(jobConfig.schema.vertexLabels.head.propertiesFromData, df)
              vertexDataCurrent <- ZIO.effect(vertexData.current.collect.toIterator)
              _                 <- vertexProcessorLive.job(jobConfig, graph, dataTypeForVertexPropertyCols, vertexDataCurrent)
              edges = List(
                new GenericRowWithSchema(Array(1L, 2L, 1), edgeSchema),
                new GenericRowWithSchema(Array(2L, 3L, 5), edgeSchema),
                new GenericRowWithSchema(Array(2L, 4L, 3), edgeSchema)
              ).toIterator
              _ <- edgeProcessorLive.job(jobConfig, graph, edgeLabel, edgePropertyKey, edges)
              g = graph.traversal()
            } yield {
              // Get vertices associated with edge similarity = 5,
              // we expect 2 edges since we add edges in both directions, hence we expect a pair of vertices repeated twice.
              g.V()
                .outE()
                .hasLabel(edgeLabel)
                .has("value", 5L)
                .toList
                .asScala
                .toList
                .flatMap(_.vertices(Direction.BOTH).asScala.map(_.property("objectId").value().toString))
            }
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
        hasSameElementsDistinct(List("ZTF17aaanypg", "ZTF19acmbxka", "ZTF19acmbxka", "ZTF17aaanypg"))
      )
    }
  )
}
