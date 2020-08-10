package com.astrolabsoftware.grafink.schema

import scala.collection.JavaConverters._

import org.apache.spark.sql.types.{ DoubleType, FloatType, IntegerType, StringType, StructField, StructType }
import org.janusgraph.core.schema.SchemaStatus
import zio.{ ZIO, ZLayer }
import zio.test.{ DefaultRunnableSpec, _ }
import zio.test.Assertion._

import com.astrolabsoftware.grafink.logging.Logger
import com.astrolabsoftware.grafink.models._
import com.astrolabsoftware.grafink.utils.JanusGraphTestEnv

object SchemaLoaderSpec extends DefaultRunnableSpec {

  def spec: ZSpec[Environment, Failure] = suite("SchemaLoaderSpec")(
    testM("Schema Loader will successfully create janusgraph schema") {
      val vertexProperties = List("rfscore", "snn")
      val dataSchema = StructType(
        List(
          StructField("rfscore", IntegerType),
          StructField("notused", StringType),
          StructField("snn", FloatType),
          StructField("random", DoubleType)
        )
      )

      val janusConfig =
        JanusGraphConfig(
          JanusGraphStorageConfig("127.0.0.1", 8182, tableName = "TestJanusGraph", List.empty),
          JanusGraphIndexBackendConfig("", "", "")
        )

      val jobConfig = GrafinkJobConfig(
        SchemaConfig(
          vertexLabels = List(VertexLabelConfig("alert", List.empty, List("rfscore", "snn"))),
          edgeLabels = List(EdgeLabelConfig("similarity", List(PropertySchema(name = "value", typ = "long")))),
          index = IndexConfig(composite = List.empty, mixed = List.empty, edge = List.empty)
        ),
        VertexLoaderConfig(10, "alert", ""),
        EdgeLoaderConfig(
          100,
          10,
          25000,
          List.empty,
          EdgeRulesConfig(SimilarityConfig("rfscore"), TwoModeSimilarityConfig(List.empty))
        )
      )
      val app =
        for {
          output <- JanusGraphTestEnv.test(janusConfig).use { graph =>
            for {
              _ <- SchemaLoader.loadSchema(graph, dataSchema)
            } yield graph.getVertexLabel("alert").mappedProperties().asScala.toList.map(_.name) ++
              graph.getEdgeLabel("similarity").mappedProperties().asScala.toList.map(_.name)
          }
        } yield output

      val logger            = Logger.test
      val schemaLoaderLayer = (ZLayer.succeed(jobConfig) ++ ZLayer.succeed(janusConfig) ++ logger) >>> SchemaLoader.live
      val layer             = schemaLoaderLayer ++ logger
      assertM(app.provideLayer(layer))(equalTo(vertexProperties ++ List("value")))
    },
    testM("Schema Loader will successfully add indices to JanusGraph") {
      val dataSchema = StructType(
        List(
          StructField("rfscore", IntegerType),
          StructField("objectId", StringType),
          StructField("notused", StringType),
          StructField("snn", FloatType),
          StructField("random", DoubleType)
        )
      )

      val edgeLabel       = "similarity"
      val objectIdIndex   = "objectIdIndex"
      val similarityIndex = "similarityIndex"

      val janusConfig =
        JanusGraphConfig(
          JanusGraphStorageConfig("127.0.0.1", 8182, tableName = "TestJanusGraph", List.empty),
          JanusGraphIndexBackendConfig("", "", "")
        )
      val jobConfig = GrafinkJobConfig(
        SchemaConfig(
          vertexLabels = List(VertexLabelConfig("alert", List.empty, List("rfscore", "snn", "objectId"))),
          edgeLabels = List(EdgeLabelConfig(edgeLabel, List(PropertySchema(name = "value", typ = "long")))),
          index = IndexConfig(
            composite = List(CompositeIndex(name = objectIdIndex, properties = List("objectId"))),
            mixed = List.empty,
            edge = List(EdgeIndex(name = similarityIndex, properties = List("value"), label = edgeLabel))
          )
        ),
        VertexLoaderConfig(10, label = "alert", ""),
        EdgeLoaderConfig(
          100,
          10,
          25000,
          List.empty,
          EdgeRulesConfig(SimilarityConfig("rfscore"), TwoModeSimilarityConfig(List.empty))
        )
      )
      case class IndexResult(name: String, status: SchemaStatus)

      val app =
        for {
          output <- JanusGraphTestEnv.test(janusConfig).use {
            graph =>
              for {
                _      <- SchemaLoader.loadSchema(graph, dataSchema)
                index1 <- ZIO.effect(graph.openManagement().getGraphIndex(objectIdIndex))
                index2 <- ZIO
                  .effect(graph.openManagement().getRelationIndex(graph.getEdgeLabel(edgeLabel), similarityIndex))
              } yield List(
                IndexResult(index1.name(), index1.getIndexStatus(graph.getPropertyKey("objectId"))),
                IndexResult(index2.name(), index2.getIndexStatus())
              )
          }
        } yield output

      val logger            = Logger.test
      val schemaLoaderLayer = (ZLayer.succeed(jobConfig) ++ ZLayer.succeed(janusConfig) ++ logger) >>> SchemaLoader.live
      val layer             = schemaLoaderLayer ++ logger
      assertM(app.provideLayer(layer))(
        hasSameElementsDistinct(
          List(
            IndexResult(objectIdIndex, SchemaStatus.ENABLED),
            IndexResult(similarityIndex, SchemaStatus.ENABLED)
          )
        )
      )
    }
  )
}
