package com.astrolabsoftware.grafink.schema

import scala.collection.JavaConverters._

import org.apache.spark.sql.types.{ DoubleType, FloatType, IntegerType, StringType, StructField, StructType }
import zio.{ ZIO, ZLayer }
import zio.blocking.Blocking
import zio.test.{ DefaultRunnableSpec, _ }
import zio.test.Assertion._
import zio.test.environment.TestConsole

import com.astrolabsoftware.grafink.logging.Logger
import com.astrolabsoftware.grafink.models.{
  JanusGraphConfig,
  JanusGraphStorageConfig,
  SchemaConfig,
  VertexLoaderConfig
}
import com.astrolabsoftware.grafink.processor.VertexProcessorSpec.{ Environment, Failure }
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
          SchemaConfig(vertexPropertyCols = vertexPeroperties, vertexLabel = "type", edgeLabels = List()),
          VertexLoaderConfig(10),
          JanusGraphStorageConfig("", 0, tableName = "test")
        )

      val app =
        for {
          output <- JanusGraphTestEnv.test(janusConfig).use { graph =>
            for {
              _ <- SchemaLoader.loadSchema(graph, dataSchema)
            } yield (graph.getVertexLabel("type").mappedProperties().asScala.toList.map(_.name))
          }
        } yield output

      val logger            = Logger.test
      val schemaLoaderLayer = (ZLayer.succeed(janusConfig) ++ logger) >>> SchemaLoader.live
      val layer             = schemaLoaderLayer ++ logger
      assertM(app.provideLayer(layer))(equalTo(vertexPeroperties))
    }
  )
}
