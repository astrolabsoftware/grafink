package com.astrolabsoftware.grafink.api.service

import scala.collection.JavaConverters._

import org.apache.tinkerpop.gremlin.structure.{ Edge, Vertex }
import org.janusgraph.core.{ EdgeLabel, JanusGraph, PropertyKey, RelationType }
import org.janusgraph.core.schema.JanusGraphIndex
import zio.{ Has, Task, URLayer, ZIO, ZLayer }

import com.astrolabsoftware.grafink.models._

object InfoService {
  type InfoService = Has[InfoService.Service]

  trait Service {
    def getGraphInfo(graph: JanusGraph): Task[SchemaInfo]
  }

  val live: URLayer[Any, InfoService] = ZLayer.succeed(new InfoServiceLive)

  def getGraphInfo(graph: JanusGraph): ZIO[InfoService, Throwable, SchemaInfo] =
    ZIO.accessM(_.get.getGraphInfo(graph))
}

final class InfoServiceLive extends InfoService.Service {

  private def getIndexType(index: JanusGraphIndex): String =
    if (index.isCompositeIndex) "Composite"
    else if (index.isMixedIndex) "Mixed"
    else "Unknown"

  private def getKeyStatus(index: JanusGraphIndex): List[String] = {
    val keys = index.getFieldKeys.toList
    keys.map(k => s"${k.name}:${index.getIndexStatus(k).name}")
  }

  override def getGraphInfo(graph: JanusGraph): Task[SchemaInfo] =
    for {
      mgmt             <- ZIO.effect(graph.openManagement())
      tVertexLabels    <- ZIO.effect(mgmt.getVertexLabels().asScala.toList)
      tEdgeLabels      <- ZIO.effect(mgmt.getRelationTypes(classOf[EdgeLabel]).asScala.toList)
      tPropertyKeys    <- ZIO.effect(mgmt.getRelationTypes(classOf[PropertyKey]).asScala.toList)
      tVertexIndexes   <- ZIO.effect(mgmt.getGraphIndexes(classOf[Vertex]).asScala.toList)
      tEdgeIndexes     <- ZIO.effect(mgmt.getGraphIndexes(classOf[Edge]).asScala.toList)
      tRelationTypes   <- ZIO.effect(mgmt.getRelationTypes(classOf[RelationType]).asScala.toList)
      tRelationIndexes <- ZIO.effect(tRelationTypes.flatMap(rt => mgmt.getRelationIndexes(rt).asScala.toList))
    } yield {
      val vertexIndexes =
        tVertexIndexes.map(v =>
          VertexIndexInfo(
            name = v.name(),
            `type` = getIndexType(v),
            isUnique = v.isUnique(),
            backingIndexName = v.getBackingIndex(),
            keyStatus = getKeyStatus(v)
          )
        )
      val edgeIndexes =
        tEdgeIndexes.map(e =>
          EdgeIndexInfo(
            name = e.name(),
            `type` = getIndexType(e),
            isUnique = e.isUnique(),
            backingIndexName = e.getBackingIndex(),
            keyStatus = getKeyStatus(e)
          )
        )
      val relationIndexes =
        tRelationIndexes.map(r =>
          RelationIndexInfo(
            name = r.name(),
            `type` = r.getType().toString,
            direction = r.getDirection().name(),
            sortKey = r.getSortKey().map(_.toString).head,
            sortOrder = r.getSortOrder().name(),
            status = r.getIndexStatus().name()
          )
        )
      SchemaInfo(
        vertexLabels = tVertexLabels.map(l =>
          VertexLabelInfo(labelName = l.name(), isPartitioned = l.isPartitioned(), isStatic = l.isStatic())
        ),
        edgeLabels = tEdgeLabels.map(l =>
          EdgeLabelInfo(
            labelName = l.name(),
            isDirected = l.isDirected(),
            isUnidirected = l.isUnidirected(),
            multiplicity = l.multiplicity().name()
          )
        ),
        propertyKeys = tPropertyKeys.map(k =>
          PropertyKeyInfo(
            propertyKeyName = k.name(),
            cardinality = k.cardinality().name(),
            dataType = k.dataType().getTypeName()
          )
        ),
        vertexIndexes = vertexIndexes,
        edgeIndexes = edgeIndexes,
        relationIndexes = relationIndexes
      )
    }
}
