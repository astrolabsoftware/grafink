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
package com.astrolabsoftware.grafink.services.reader

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.DataFrame
import zio.{ Has, RIO, URLayer, ZIO, ZLayer }
import zio.logging.{ log, Logging }

import com.astrolabsoftware.grafink.Job.{ JobTime, SparkEnv }
import com.astrolabsoftware.grafink.common.PartitionManager
import com.astrolabsoftware.grafink.models.{ IDManagerConfig, ReaderConfig }
import com.astrolabsoftware.grafink.models.config.Config

object Reader {

  type ReaderService = Has[Reader.Service]

  trait Service {
    def config: ReaderConfig
    def read(partitionManager: PartitionManager): RIO[Logging, DataFrame]
  }

  val live: URLayer[SparkEnv with Logging with Has[ReaderConfig], ReaderService] =
    ZLayer.fromEffect(
      for {
        spark      <- ZIO.access[SparkEnv](_.get.sparkEnv)
        readerConf <- Config.readerConfig
      } yield new Service {
        override def config: ReaderConfig = readerConf

        override def read(partitionManager: PartitionManager): RIO[Logging, DataFrame] =
          ZIO
            .effect(FileSystem.get(spark.sparkContext.hadoopConfiguration))
            .bracket(fs =>
              ZIO
                .effect(fs.close())
                .fold(failure => log.error(s"Error closing filesystem: $failure"), _ => log.info(s""))
            )(fs =>
              for {
                readPaths <- partitionManager.getValidPartitionPathStrings(readerConf.basePath, fs)
                df = if (readPaths.isEmpty) {
                  spark.emptyDataFrame
                } else {
                  spark.read
                    .option("basePath", config.basePath)
                    .format(config.format.toString)
                    .load(readPaths: _*)
                }
              } yield df
            )
      }
    )

  def read(partitionManager: PartitionManager): RIO[ReaderService with Logging, DataFrame] =
    RIO.accessM(_.get.read(partitionManager))

}
