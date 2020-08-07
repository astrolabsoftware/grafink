/*
 * Copyright 2020 AstroLab Software
 * Author: Yash Datta
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.astrolabsoftware.grafink.api.service

import scala.collection.JavaConverters._

import io.circe.generic.auto._
import org.apache.tinkerpop.gremlin.structure._
import org.http4s.HttpRoutes
import org.http4s.circe.CirceEntityCodec._
import org.http4s.dsl.Http4sDsl
import org.janusgraph.core.{ EdgeLabel, PropertyKey, RelationType }
import org.janusgraph.core.schema.JanusGraphIndex
import zio._
import zio.interop.catz._
import zio.logging.Logging

import com.astrolabsoftware.grafink.JanusGraphEnv
import com.astrolabsoftware.grafink.api.service.JanusGraphConnectionManager.JanusGraphConnManagerService
import com.astrolabsoftware.grafink.models._

final case class MgmtService[R <: JanusGraphConnManagerService with Logging](janusGraphConfig: JanusGraphConfig) {

  type GraphTask[A] = RIO[R, A]

  val dsl = new Http4sDsl[GraphTask] {}
  import dsl._

  val emptySchemaInfo = SchemaInfo(
    vertexLabels = List.empty,
    edgeLabels = List.empty,
    propertyKeys = List.empty,
    vertexIndexes = List.empty,
    edgeIndexes = List.empty,
    relationIndexes = List.empty
  )

  private def getIndexType(index: JanusGraphIndex): String =
    if (index.isCompositeIndex) "Composite"
    else if (index.isMixedIndex) "Mixed"
    else "Unknown"

  private def getKeyStatus(index: JanusGraphIndex): List[String] = {
    val keys = index.getFieldKeys.toList
    keys.map(k => s"${k.name}:${index.getIndexStatus(k).name}")
  }

  def routes: HttpRoutes[GraphTask] =
    HttpRoutes.of[GraphTask] {
      case req @ POST -> Root / "info" =>
        req.decode[InfoRequest] { request =>
          val config = // Use table name from request
            janusGraphConfig.copy(storage = janusGraphConfig.storage.copy(tableName = request.tableName))
          val response = (for {
            graph            <- JanusGraphConnectionManager.getOrCreateGraphInstance(config)(JanusGraphEnv.withHBaseStorageRead)
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
            InfoResponse(
              schema = SchemaInfo(
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
            )
          }) catchAll (t => ZIO.succeed(InfoResponse(emptySchemaInfo, error = s"$t")))
          Ok(response)
        }
    }
}
