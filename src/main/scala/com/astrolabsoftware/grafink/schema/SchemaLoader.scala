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

import zio.{Has, URLayer, ZIO, ZLayer}
import zio.logging.Logging

import com.astrolabsoftware.grafink.JanusGraphEnv.JanusGraphEnv
import com.astrolabsoftware.grafink.models.JanusGraphConfig
import com.astrolabsoftware.grafink.models.config.Config

object SchemaLoader {

  type SchemaLoaderService = Has[SchemaLoader.Service]

  trait Service {
    def loadSchema(): ZIO[Logging with JanusGraphEnv, Throwable, Unit]
  }

  val live: URLayer[JanusGraphEnv with Logging with Has[JanusGraphConfig], SchemaLoaderService] =
    ZLayer.fromEffect(
      for {
        graph <- ZIO.access[JanusGraphEnv](_.get.graph)
        janusGraphConfig <- Config.janusGraphConfig
      } yield new Service {
        override def loadSchema(): ZIO[Logging with JanusGraphEnv, Throwable, Unit] = {

          val edgeLabels = janusGraphConfig.schema.edgeLabels
          val e = edgeLabels.map(graph.makeEdgeLabel)

          val vertexPropertyCols = janusGraphConfig.schema.vertexPropertyCols

          for {
            vertextLabel <- ZIO.effect(graph.makeVertexLabel(janusGraphConfig.schema.vertexLabel).make)
            // TODO: Detect the data type from input data types
            vertexProperties <- ZIO.effect(vertexPropertyCols.map(m => graph.makePropertyKey(m).dataType(classOf[String]).make))
            _ = graph.addProperties(vertextLabel, vertexProperties: _*)
            _ <- ZIO.effect(e.foreach(_.make))
            _ <- ZIO.effect(graph.tx.commit)
          } yield ()
        }
      }
    )

  def loadSchema(): ZIO[SchemaLoaderService with Logging with JanusGraphEnv, Throwable, Unit] =
    ZIO.accessM(_.get.loadSchema())

}
