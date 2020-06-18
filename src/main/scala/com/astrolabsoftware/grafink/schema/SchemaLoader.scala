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
package com.astrolabsoftware.grafink.schema

import scala.collection.JavaConverters._

import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality
import org.janusgraph.core.JanusGraph
import org.janusgraph.core.Multiplicity._
import zio.{Has, URLayer, ZIO, ZLayer}
import zio.logging.{log, Logging}

import com.astrolabsoftware.grafink.common.Utils
import com.astrolabsoftware.grafink.models.JanusGraphConfig
import com.astrolabsoftware.grafink.models.config.Config

object SchemaLoader {

  type SchemaLoaderService = Has[SchemaLoader.Service]

  trait Service {
    def loadSchema(graph: JanusGraph, dataSchema: StructType): ZIO[Logging, Throwable, Unit]
  }

  val live: URLayer[Logging with Has[JanusGraphConfig], SchemaLoaderService] =
    ZLayer.fromEffect(
      for {
        janusGraphConfig <- Config.janusGraphConfig
      } yield new Service {

        def load(graph: JanusGraph, dataSchema: StructType): ZIO[Logging, Throwable, Unit] = {
          val edgeLabels = janusGraphConfig.schema.edgeLabels

          val vertexPropertyCols = janusGraphConfig.schema.vertexPropertyCols
          val dataTypeForVertexPropertyCols: Map[String, DataType] =
            dataSchema.fields.map(f => f.name -> f.dataType).toMap

          for {
            vertextLabel <- ZIO.effect(graph.makeVertexLabel(janusGraphConfig.schema.vertexLabel).make)
            // TODO: Detect the data type from input data types
            vertexProperties <- ZIO.effect(
              vertexPropertyCols.map { m =>
                val dType = Utils.getClassTag(dataTypeForVertexPropertyCols(m))
                graph.makePropertyKey(m).dataType(dType).make
              }
            )
            _ <- ZIO.effect(graph.addProperties(vertextLabel, vertexProperties: _*))
            // TODO make this better
            _ <- ZIO.collectAll_(edgeLabels.map(l =>
              for {
                label <- ZIO.effect(graph.makeEdgeLabel(l.name).multiplicity(MULTI).make)
                labelWithProperty <- if (!l.properties.isEmpty) {
                  // An edgelabel cardinality can only be SINGLE
                  val k = l.properties("key")
                  val v = l.properties("typ")
                  for {
                    // scalastyle:off
                    // https://github.com/JanusGraph/janusgraph/blob/master/janusgraph-core/src/main/java/org/janusgraph/graphdb/transaction/StandardJanusGraphTx.java#L924
                    // scalastyle:on
                    property <- ZIO.effect(graph.makePropertyKey(k).dataType(Utils.getClassTagFromString(v)).cardinality(Cardinality.single).make)
                    editedLabel <- ZIO.effect(graph.addProperties(label, property))
                  } yield editedLabel
                } else ZIO.succeed(label)
              } yield labelWithProperty)
            )
            _ <- ZIO.effect(graph.tx.commit).tapBoth(
              e => log.info(s"Something went wrong while creating schema $e"), s => log.info(s"Successfully created Table schema")
            )
          } yield ()
        }

        // We can assume a flat schema here since we already flatten the schema while reading the data
        override def loadSchema(graph: JanusGraph, dataSchema: StructType): ZIO[Logging, Throwable, Unit] = {

          val managedMgmt =
            ZIO.effect(graph.openManagement()).toManaged(mgmt => ZIO.effect(mgmt.commit()).catchAll(f => log.error(s"Error committing mgmt $f")))

          for {
            vertexLabels <- managedMgmt.use(mgmt => ZIO.effect(mgmt.getVertexLabels.asScala.toList.map(_.name)))
            _ <- if (vertexLabels.size == 0) {
              // We do not have schema
              for {
               _ <- log.info(s"Starting to load schema")
               _ <- load(graph, dataSchema)
              } yield ()
            } else {
              log.info(s"""Found vertexLabels (${vertexLabels.mkString(",")}) in the target table, skipping schema loading""")
            }
          } yield ()
        }
      }
    )

  def loadSchema(
    graph: JanusGraph,
    dataSchema: StructType
  ): ZIO[SchemaLoaderService with Logging, Throwable, Unit] =
    ZIO.accessM(_.get.loadSchema(graph, dataSchema))

}
