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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import zio.{ Has, RIO, URLayer, ZIO, ZLayer }
import zio.logging.{ log, Logging }

import com.astrolabsoftware.grafink.Job.SparkEnv
import com.astrolabsoftware.grafink.common.{ PartitionManager, Utils }
import com.astrolabsoftware.grafink.models.GrafinkException.NoDataException
import com.astrolabsoftware.grafink.models.ReaderConfig
import com.astrolabsoftware.grafink.models.config.Config

/**
 * Spark data reader
 */
object Reader {

  type ReaderService = Has[Reader.Service]

  trait Service {
    def config: ReaderConfig
    def read(basePath: String, partitionManager: PartitionManager): RIO[Logging, DataFrame]
    def readAndProcess(basePath: String, partitionManager: PartitionManager): RIO[Logging, DataFrame]
    def readAndProcess(partitionManager: PartitionManager): RIO[Logging, DataFrame]
  }

  val live: URLayer[SparkEnv with Logging with Has[ReaderConfig], ReaderService] =
    ZLayer.fromEffect(
      for {
        spark      <- ZIO.access[SparkEnv](_.get.sparkEnv)
        readerConf <- Config.readerConfig
      } yield new Service {
        override def config: ReaderConfig = readerConf

        override def read(basePath: String, partitionManager: PartitionManager): RIO[Logging, DataFrame] =
          Utils.withFileSystem {
            fs =>
              for {
                readPaths <- partitionManager.getValidPartitionPathStrings(basePath, fs)
                _         <- log.info(s"Reading data from paths: ${readPaths.mkString}")
                df <- if (readPaths.isEmpty) {
                  ZIO.fail(
                    NoDataException(
                      s"No data at basepath ${readerConf.basePath} for startdate = ${partitionManager.startDate}, duration = ${partitionManager.duration}"
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
              } yield df
          }(spark)

        override def readAndProcess(basePath: String, partitionManager: PartitionManager): RIO[Logging, DataFrame] =
          for {
            df <- read(basePath, partitionManager)
            colsToSelect = config.keepCols.map(col(_)) ++ PartitionManager.partitionColumns.map(col)
            colsRenamed  = config.keepColsRenamed.map(c => col(c.f).as(c.t))
            dfPruned     = df.select(colsToSelect ++ colsRenamed: _*)
          } yield dfPruned

        override def readAndProcess(partitionManager: PartitionManager): RIO[Logging, DataFrame] =
          readAndProcess(readerConf.basePath, partitionManager)
      }
    )

  def readAndProcess(partitionManager: PartitionManager): RIO[ReaderService with Logging, DataFrame] =
    RIO.accessM(_.get.readAndProcess(partitionManager))

  def readAndProcess(basePath: String, partitionManager: PartitionManager): RIO[ReaderService with Logging, DataFrame] =
    RIO.accessM(_.get.readAndProcess(basePath, partitionManager))

  def read(basePath: String, partitionManager: PartitionManager): RIO[ReaderService with Logging, DataFrame] =
    RIO.accessM(_.get.read(basePath, partitionManager))
}
