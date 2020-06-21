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
package com.astrolabsoftware.grafink

import java.time.LocalDate

import org.apache.spark.sql.{ DataFrame, SparkSession }
import zio._
import zio.logging.Logging

import com.astrolabsoftware.grafink.common.PartitionManager
import com.astrolabsoftware.grafink.models.config._
import com.astrolabsoftware.grafink.processor.{ EdgeProcessor, VertexProcessor }
import com.astrolabsoftware.grafink.processor.EdgeProcessor.EdgeProcessorService
import com.astrolabsoftware.grafink.processor.VertexProcessor.VertexProcessorService
import com.astrolabsoftware.grafink.processor.edgerules.SimilarityClassifer
import com.astrolabsoftware.grafink.schema.SchemaLoader
import com.astrolabsoftware.grafink.schema.SchemaLoader.SchemaLoaderService
import com.astrolabsoftware.grafink.services.IDManagerSparkService.IDManagerSparkService
import com.astrolabsoftware.grafink.services.reader.Reader
import com.astrolabsoftware.grafink.services.reader.Reader.ReaderService

// The core application
object Job {

  case class JobTime(day: LocalDate, duration: Int)

  case class VertexData(loaded: DataFrame, current: DataFrame)

  type SparkEnv = Has[SparkEnv.Service]
  type RunEnv =
    SparkEnv
      with GrafinkConfig
      with SchemaLoaderService
      with ReaderService
      with IDManagerSparkService
      with VertexProcessorService
      with EdgeProcessorService
      with Logging
      with ZEnv

  val process: (JobTime, SparkSession) => ZIO[RunEnv, Throwable, Unit] =
    (jobTime, spark) =>
      for {
        janusGraphConfig <- Config.janusGraphConfig
        partitionManager = PartitionManager(jobTime.day, jobTime.duration)
        // read current data
        df <- Reader.read(partitionManager)
        _ <- JanusGraphEnv
          .hbaseBasic(janusGraphConfig)
          .use(graph =>
            for {
              // load schema
              _ <- SchemaLoader.loadSchema(graph, df.schema)
            } yield ()
          )
        // Generate Ids for the data
        idManager  <- ZIO.access[IDManagerSparkService](_.get)
        vertexData <- idManager.process(df, janusGraphConfig.storage.tableName)
        // Process vertices
        _ <- VertexProcessor.process(jobTime, vertexData.current)
        // Process Edges
        _ <- EdgeProcessor.process(
          vertexData,
          List(new SimilarityClassifer(janusGraphConfig.edgeLoader.rules.similarityClassifer))
        )
      } yield ()

  /**
   * Runs the spark job to load data into JanusGraph
   */
  val runGrafinkJob: JobTime => ZIO[RunEnv, Throwable, Unit] =
    jobTime =>
      for {
        spark <- ZIO.access[SparkEnv](_.get.sparkEnv)
        result <- process(jobTime, spark)
          .ensuring(
            ZIO
              .effect(spark.stop())
              .fold(
                failure => zio.console.putStrLn(s"Error stopping SparkSession: $failure"),
                _ => zio.console.putStrLn(s"SparkSession stopped")
              )
          )
        _ <- zio.console.putStrLn(s"Executed grafink job with spark ${spark.version}: $result")
      } yield ()
}
