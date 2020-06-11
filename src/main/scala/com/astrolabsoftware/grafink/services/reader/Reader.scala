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
import org.apache.spark.sql.functions._
import zio.{ Has, RIO, URLayer, ZIO, ZLayer }
import zio.logging.{ log, Logging }

import com.astrolabsoftware.grafink.Job.SparkEnv
import com.astrolabsoftware.grafink.common.PartitionManager
import com.astrolabsoftware.grafink.models.GrafinkException.NoDataException
import com.astrolabsoftware.grafink.models.ReaderConfig
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
                _         <- log.info(s"Reading data from paths: ${readPaths.mkString}")
                df <- if (readPaths.isEmpty) {
                  ZIO.fail(
                    NoDataException(
                      s"No data at basepath ${readerConf.basePath} for startdate = ${partitionManager.startDate} and duration = ${partitionManager.duration}"
                    )
                  )
                } else {
                  ZIO.effect(
                    spark.read
                      .option("basePath", config.basePath)
                      .format(config.format.toString)
                      .load(readPaths: _*)
                  )
                }
                colsToSelect = config.keepCols.map(col(_))
                colsRenamed  = config.keepColsRenamed.map(c => col(c.f).as(c.t))
                dfPruned     = df.select(colsToSelect ++ colsRenamed: _*)
              } yield dfPruned
            )
      }
    )

  def read(partitionManager: PartitionManager): RIO[ReaderService with Logging, DataFrame] =
    RIO.accessM(_.get.read(partitionManager))

}
