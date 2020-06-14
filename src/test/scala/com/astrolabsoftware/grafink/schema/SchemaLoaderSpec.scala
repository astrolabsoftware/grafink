package com.astrolabsoftware.grafink.schema

import scala.collection.JavaConverters._

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
      val app =
        for {
          graph <- JanusGraphTestEnv.graph
          _     <- SchemaLoader.loadSchema(graph)
        } yield (graph.getVertexLabel("type").mappedProperties().asScala.toList.map(_.name))

      val janusConfig = ZLayer.succeed(
        JanusGraphConfig(
          SchemaConfig(vertexPropertyCols = vertexPeroperties, vertexLabel = "type", edgeLabels = List()),
          VertexLoaderConfig(10),
          JanusGraphStorageConfig("", 0, tableName = "test")
        )
      )
      val logger            = Logger.test
      val janusLayer        = (janusConfig ++ Blocking.live) >>> JanusGraphTestEnv.test
      val schemaLoaderLayer = (janusConfig ++ janusLayer ++ logger) >>> SchemaLoader.live
      val layer             = schemaLoaderLayer ++ logger ++ janusLayer
      assertM(app.provideLayer(layer))(equalTo(vertexPeroperties))
    }
  )
}
