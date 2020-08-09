package com.astrolab.software.grafink.api.service

import scala.collection.JavaConverters._

import zio._
import zio.test._
import zio.test.Assertion._

import com.astrolabsoftware.grafink.JanusGraphEnv
import com.astrolabsoftware.grafink.api.service.JanusGraphConnectionManager
import com.astrolabsoftware.grafink.logging.Logger
import com.astrolabsoftware.grafink.models.{
  AppConfig,
  JanusGraphConfig,
  JanusGraphIndexBackendConfig,
  JanusGraphStorageConfig
}

object JanusGraphConnectionManagerSpec extends DefaultRunnableSpec {

  def spec: ZSpec[Environment, Failure] = suite("JanusGraphConnectionManagerSpec")(
    testM("JanusGraphConnectionManager will correctly cache the graph object") {
      val table1 = "TestJanusGraph1"
      val table2 = "TestJanusGraph2"
      val janusConfig =
        JanusGraphConfig(
          JanusGraphStorageConfig("127.0.0.1", 8182, tableName = table1, List.empty),
          JanusGraphIndexBackendConfig("", "", "")
        )
      val janusConfig2 = janusConfig.copy(storage = janusConfig.storage.copy(tableName = table2))
      val app =
        for {
          g1        <- JanusGraphConnectionManager.getOrCreateGraphInstance(janusConfig)(JanusGraphEnv.inMemoryStorage)
          mgmt      <- ZIO.effect(g1.openManagement())
          vLabel1   <- ZIO.effect(mgmt.makeVertexLabel("alert1").make)
          property1 <- ZIO.effect(mgmt.makePropertyKey("prop1").dataType(classOf[java.lang.String]).make)
          _         <- ZIO.effect(mgmt.addProperties(vLabel1, property1))
          _         <- ZIO.effect(mgmt.commit())
          g2        <- JanusGraphConnectionManager.getOrCreateGraphInstance(janusConfig2)(JanusGraphEnv.inMemoryStorage)
          mgmt      <- ZIO.effect(g2.openManagement())
          vLabel2   <- ZIO.effect(mgmt.makeVertexLabel("alert2").make)
          property2 <- ZIO.effect(mgmt.makePropertyKey("prop2").dataType(classOf[java.lang.String]).make)
          _         <- ZIO.effect(mgmt.addProperties(vLabel2, property2))
          _         <- ZIO.effect(mgmt.commit())

          // get g1 again
          g1again <- JanusGraphConnectionManager.getOrCreateGraphInstance(janusConfig)(JanusGraphEnv.inMemoryStorage)
          g2again <- JanusGraphConnectionManager.getOrCreateGraphInstance(janusConfig2)(JanusGraphEnv.inMemoryStorage)

        } yield g1again.getVertexLabel("alert1").mappedProperties().asScala.toList.map(_.name) ++
          g2again.getVertexLabel("alert2").mappedProperties().asScala.toList.map(_.name)

      val logger      = Logger.test
      val configLayer = ZLayer.succeed(AppConfig(9073, 2))
      val layer       = ((configLayer ++ logger) >>> JanusGraphConnectionManager.live) ++ logger
      assertM(app.provideLayer(layer))(equalTo(List("prop1", "prop2")))
    }
  )
}
