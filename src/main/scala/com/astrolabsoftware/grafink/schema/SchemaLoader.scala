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

import org.apache.spark.sql.types.{
  BinaryType,
  BooleanType,
  ByteType,
  DataType,
  DoubleType,
  FloatType,
  IntegerType,
  LongType,
  StringType,
  StructType
}
import org.janusgraph.core.JanusGraph
import zio.{ Has, URLayer, ZIO, ZLayer }
import zio.logging.Logging

import com.astrolabsoftware.grafink.JanusGraphEnv.JanusGraphEnv
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
        // We can assume a flat schema here since we already flatten the schema while reading the data
        override def loadSchema(graph: JanusGraph, dataSchema: StructType): ZIO[Logging, Throwable, Unit] = {

          val edgeLabels = janusGraphConfig.schema.edgeLabels

          val vertexPropertyCols = janusGraphConfig.schema.vertexPropertyCols
          val dataTypeForVertexPropertyCols: Map[String, DataType] =
            dataSchema.fields.map(f => f.name -> f.dataType).toMap

          for {
            vertextLabel <- ZIO.effect(graph.makeVertexLabel(janusGraphConfig.schema.vertexLabel).make)
            _            <- ZIO.effect(graph.tx.commit)
            // TODO: Detect the data type from input data types
            vertexProperties <- ZIO.effect(
              vertexPropertyCols.map { m =>
                val dType = Utils.getClassTag(dataTypeForVertexPropertyCols(m))
                graph.makePropertyKey(m).dataType(dType).make
              }
            )
            _ = graph.addProperties(vertextLabel, vertexProperties: _*)
            _ <- ZIO.effect(graph.tx.commit)
            _ <- ZIO.effect(edgeLabels.map(graph.makeEdgeLabel).foreach(_.make))
            _ <- ZIO.effect(graph.tx.commit)
          } yield ()
        }
      }
    )

  def loadSchema(
    graph: JanusGraph,
    dataSchema: StructType
  ): ZIO[SchemaLoaderService with Logging with JanusGraphEnv, Throwable, Unit] =
    ZIO.accessM(_.get.loadSchema(graph, dataSchema))

}
