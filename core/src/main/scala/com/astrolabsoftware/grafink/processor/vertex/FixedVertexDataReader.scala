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
package com.astrolabsoftware.grafink.processor.vertex

import java.io.File

import scala.io.Source

import com.github.tototoshi.csv.CSVReader
import zio.{ Has, URLayer, ZIO, ZLayer }
import zio.logging.{ log, Logging }

import com.astrolabsoftware.grafink.common.Utils
import com.astrolabsoftware.grafink.models.{ FixedVertex, FixedVertexProperty, VertexLoaderConfig }

object FixedVertexDataReader {

  type FixedVertexDataReader = Has[FixedVertexDataReader.Service]

  trait Service {
    def readFixedVertexData(config: VertexLoaderConfig): ZIO[Logging, Throwable, List[FixedVertex]]
  }

  val live: URLayer[Logging, FixedVertexDataReader] =
    ZLayer.succeed {
      new Service {
        override def readFixedVertexData(config: VertexLoaderConfig): ZIO[Logging, Throwable, List[FixedVertex]] =
          for {
            path   <- ZIO.effect((new File(getClass().getResource(config.fixedVertices).getPath)).getAbsolutePath)
            _      <- log.info(s"Reading fixed vertices data from $path")
            reader <- ZIO.effect(CSVReader.open(path))
            it = reader.iterator.toList
          } yield {
            it.map { r =>
              val colNum        = r.size
              val numProperties = (colNum - 2) % 3
              val id            = r(0).toLong
              val label         = r(1)
              val properties = (0 to numProperties).map { i =>
                val propIndex   = 2 + (i * 3)
                val propertyVal = Utils.stringToValueType(r(propIndex + 2), r(propIndex + 1))
                FixedVertexProperty(r(propIndex), r(propIndex + 1), propertyVal)
              }
              FixedVertex(id = id, label = label, properties.toList)
            }
          }
      }
    }

  def readFixedVertexData(
    config: VertexLoaderConfig
  ): ZIO[FixedVertexDataReader with Logging, Throwable, List[FixedVertex]] =
    ZIO.accessM(_.get.readFixedVertexData(config))

}
