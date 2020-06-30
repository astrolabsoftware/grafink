package com.astrolabsoftware.grafink.models

import zio.test._
import zio.test.Assertion._

import com.astrolabsoftware.grafink.logging.Logger
import com.astrolabsoftware.grafink.models.config.{ Config, GrafinkConfig }

object ConfigSpec extends DefaultRunnableSpec {
  def spec: ZSpec[Environment, Failure] =
    suite("ConfigSpec")(
      testM("ReaderConfig is parsed correctly") {
        val path = getClass.getResource("/application.conf").getPath

        val layer = Logger.test >>> Config.live(path)

        val cfg = Config.readerConfig
        assertM(cfg.provideLayer(layer))(
          equalTo(
            ReaderConfig(
              basePath = "/test/base/path",
              format = Parquet,
              keepCols = List("objectId", "schemavsn"),
              keepColsRenamed = List(
                RenameColumn(f = "mulens.class_1", t = "mulens_class_1"),
                RenameColumn(f = "mulens.class_2", t = "mulens_class_2")
              )
            )
          )
        )
      },
      testM("HBaseConfig is parsed correctly") {

        val path = getClass.getResource("/application.conf").getPath

        val layer = Logger.test >>> Config.live(path)

        val cfg = Config.hbaseConfig
        assertM(cfg.provideLayer(layer))(equalTo(HBaseConfig(HBaseZookeeperConfig(quoram = "localhost"))))
      },
      testM("JanusGraphConfig is parsed correctly") {

        val path = getClass.getResource("/application.conf").getPath

        val layer = Logger.test >>> Config.live(path)

        val cfg = Config.janusGraphConfig
        assertM(cfg.provideLayer(layer))(
          equalTo(
            JanusGraphConfig(
              SchemaConfig(
                vertexPropertyCols = List("rfscore", "snnscore"),
                vertexLabel = "type",
                edgeLabels = List(EdgeLabelConfig("similarity", Map("key" -> "value", "typ" -> "int")))
              ),
              VertexLoaderConfig(10),
              EdgeLoaderConfig(100, 10, 25000, EdgeRulesConfig(SimilarityConfig("rfscore OR objectId"))),
              JanusGraphStorageConfig("127.0.0.1", 8182, tableName = "TestJanusGraph")
            )
          )
        )
      },
      testM("Invalid config file throws") {
        val path  = getClass.getResource("/invalidapplication.conf").getPath
        val layer = Logger.test >>> Config.live(path)
        val cfg   = Config.readerConfig
        assertM(cfg.provideLayer(layer).run)(
          fails(isSubtype[pureconfig.error.ConfigReaderException[GrafinkConfig]](anything))
        )
      }
    )
}
