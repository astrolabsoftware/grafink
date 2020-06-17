package com.astrolabsoftware.grafink.schema

import scala.collection.JavaConverters._

import org.apache.spark.sql.types.{DoubleType, FloatType, IntegerType, StringType, StructField, StructType}
import zio.{ZIO, ZLayer}
import zio.test.{DefaultRunnableSpec, _}
import zio.test.Assertion._

import com.astrolabsoftware.grafink.logging.Logger
import com.astrolabsoftware.grafink.models.{EdgeLabelConfig, EdgeLoaderConfig, EdgeRulesConfig, JanusGraphConfig, JanusGraphStorageConfig, SchemaConfig, SimilarityConfig, VertexLoaderConfig}
import com.astrolabsoftware.grafink.utils.JanusGraphTestEnv

object SchemaLoaderSpec extends DefaultRunnableSpec {

  def spec: ZSpec[Environment, Failure] = suite("SchemaLoaderSpec")(
    testM("Schema Loader will successfully create janusgraph schema") {
      val vertexPeroperties = List("rfscore", "snn")
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
          SchemaConfig(
            vertexPropertyCols = List("rfscore", "snn"),
            vertexLabel = "type",
            edgeLabels = List(EdgeLabelConfig("similarity", Map("key"->"value", "typ" -> "long")))),
          VertexLoaderConfig(10),
          EdgeLoaderConfig(100, EdgeRulesConfig(SimilarityConfig(List("rfscore"), 10))),
          JanusGraphStorageConfig("127.0.0.1", 8182, tableName = "TestJanusGraph")
        )

      val app =
        for {
          output <- JanusGraphTestEnv.test(janusConfig).use { graph =>
            for {
              _ <- SchemaLoader.loadSchema(graph, dataSchema)
            } yield graph.getVertexLabel("type").mappedProperties().asScala.toList.map(_.name) ++
              graph.getEdgeLabel("similarity").mappedProperties().asScala.toList.map(_.name)
          }
        } yield output

      val logger            = Logger.test
      val schemaLoaderLayer = (ZLayer.succeed(janusConfig) ++ logger) >>> SchemaLoader.live
      val layer             = schemaLoaderLayer ++ logger
      assertM(app.provideLayer(layer))(equalTo(vertexPeroperties ++ List("value")))
    }
  )
}
