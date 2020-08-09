package com.astrolab.software.grafink.api.service

import org.apache.tinkerpop.gremlin.structure.{ Direction, Vertex }
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality
import org.janusgraph.core.Multiplicity.MULTI
import org.janusgraph.core.schema.SchemaAction
import zio._
import zio.test._
import zio.test.Assertion._

import com.astrolabsoftware.grafink.JanusGraphEnv
import com.astrolabsoftware.grafink.api.service.InfoService
import com.astrolabsoftware.grafink.logging.Logger
import com.astrolabsoftware.grafink.models._

object InfoServiceSpec extends DefaultRunnableSpec {

  def spec: ZSpec[Environment, Failure] = suite("InfoServiceSpec")(
    testM("InfoService will correctly return graph info") {
      val tableName       = "TestJanusGraph"
      val edgeLabel       = "similarity"
      val objectIdIndex   = "objectIdIndex"
      val similarityIndex = "similarityIndex"
      val vertexLabel     = "alert"
      val vertexPropertyCols =
        List(("rfscore", classOf[java.lang.Double]), ("snn", classOf[java.lang.Double]), ("objectId", classOf[String]))

      val janusConfig = JanusGraphConfig(
        storage = JanusGraphStorageConfig(
          host = "127.0.0.1",
          port = 8182,
          tableName = tableName,
          extraConf = List.empty
        ),
        indexBackend = JanusGraphIndexBackendConfig(name = "", indexName = "", host = "")
      )

      val app =
        for {
          output <- JanusGraphEnv.inmemory(janusConfig).use {
            graph =>
              for {
                mgmt <- ZIO.effect(graph.openManagement())
                // Add a vertex label
                vLabel <- ZIO.effect(mgmt.makeVertexLabel(vertexLabel).make)
                vertexProperties = vertexPropertyCols.map(m => mgmt.makePropertyKey(m._1).dataType(m._2).make)
                _ <- ZIO.effect(mgmt.addProperties(vLabel, vertexProperties: _*))
                // Add edge label
                eLabel <- ZIO.effect(mgmt.makeEdgeLabel(edgeLabel).multiplicity(MULTI).make)
                eProperty <- ZIO.effect(
                  mgmt
                    .makePropertyKey("value")
                    .dataType(classOf[java.lang.Integer])
                    .cardinality(Cardinality.single)
                    .make
                )
                _ <- ZIO.effect(mgmt.addProperties(eLabel, eProperty))
                // Add index
                objectIdPropertyKey = vertexProperties.filter(x => x.name == "objectId").head
                index1 <- ZIO.effect(
                  mgmt.buildIndex(objectIdIndex, classOf[Vertex]).addKey(objectIdPropertyKey).buildCompositeIndex()
                )

                // Add edge index
                edgeLabelV <- ZIO.effect(mgmt.getEdgeLabel(edgeLabel))
                index2     <- ZIO.effect(mgmt.buildEdgeIndex(edgeLabelV, similarityIndex, Direction.BOTH, eProperty))
                _          <- ZIO.effect(mgmt.commit())

                // enable Index
                mgmt <- ZIO.effect(graph.openManagement())
                _    <- ZIO.effect(mgmt.updateIndex(mgmt.getGraphIndex(index1.name()), SchemaAction.ENABLE_INDEX))
                _    <- ZIO.effect(mgmt.commit())

                // Get schema
                schema <- InfoService.getGraphInfo(graph)
              } yield schema
          }
        } yield output

      val logger = Logger.test
      // val configLayer = ZLayer.succeed(AppConfig(9073, 1))
      // val janusGraphConnectionManagerLayer = (configLayer ++ Logger.live) >>> JanusGraphConnectionManager.live
      val layer = InfoService.live ++ logger
      assertM(app.provideLayer(layer))(
        equalTo(
          SchemaInfo(
            vertexLabels = List(VertexLabelInfo(vertexLabel, false, false)),
            edgeLabels = List(EdgeLabelInfo(edgeLabel, true, false, "MULTI")),
            propertyKeys = List(
              PropertyKeyInfo("rfscore", "SINGLE", "java.lang.Double"),
              PropertyKeyInfo("snn", "SINGLE", "java.lang.Double"),
              PropertyKeyInfo("objectId", "SINGLE", "java.lang.String"),
              PropertyKeyInfo("value", "SINGLE", "java.lang.Integer")
            ),
            vertexIndexes = List(
              VertexIndexInfo(objectIdIndex, "Composite", false, "internalindex", List("objectId:ENABLED"))
            ),
            edgeIndexes = List.empty,
            relationIndexes = List(
              RelationIndexInfo(similarityIndex, edgeLabel, "BOTH", "value", "asc", "ENABLED")
            )
          )
        )
      )
    }
  )
}
