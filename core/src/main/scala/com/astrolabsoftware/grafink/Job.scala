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

import org.apache.spark.sql.DataFrame
import zio._
import zio.logging.{ log, Logging }

import com.astrolabsoftware.grafink.common.{ PaddedPartitionManager, PartitionManagerImpl, Utils }
import com.astrolabsoftware.grafink.models.GrafinkJanusGraphConfig
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

/**
 * The core application logic
 */
object Job {

  case class JobTime(day: LocalDate, duration: Int)

  /**
   * @param loaded The data already loaded in janusgraph
   * @param current The data to be loaded for the current run
   */
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

  val process: JobTime => ZIO[RunEnv, Throwable, Unit] =
    jobTime =>
      for {
        jobConfig        <- Config.jobConfig
        janusGraphConfig <- Config.janusGraphConfig
        partitionManager = PaddedPartitionManager(jobTime.day, jobTime.duration)
        // read current data
        df <- Reader.readAndProcess(partitionManager)
        _ <- JanusGraphEnv
          .hbaseBasic(GrafinkJanusGraphConfig(jobConfig, janusGraphConfig))
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
        _ <- VertexProcessor.process(vertexData.current)
        // Process Edges
        _ <- EdgeProcessor.process(
          vertexData,
          List(new SimilarityClassifer(jobConfig.edgeLoader.rules.similarityClassifer))
        )
      } yield ()

  val delete: JobTime => ZIO[RunEnv, Throwable, Unit] =
    jobTime =>
      for {
        janusGraphConfig <- Config.janusGraphConfig
        idManagerConfig  <- Config.idManagerConfig
        basePath         = s"${idManagerConfig.spark.dataPath}/${janusGraphConfig.storage.tableName}"
        partitionManager = PartitionManagerImpl(jobTime.day, jobTime.duration)
        // read data for the specified date from idmanager data path
        df <- Reader.read(basePath, partitionManager)
        // Delete vertices
        _ <- VertexProcessor.delete(df)
        _ <- if (idManagerConfig.spark.clearOnDelete) {
          // Clear IdManagerData as well
          Utils.withFileSystem(fs => partitionManager.deletePartitions(basePath, fs))(df.sparkSession)
        } else {
          log.info(s"Skipping deleting IDManager data")
        }
      } yield ()

  /**
   * Runs the spark job with specified job
   */
  val runJob: ZIO[RunEnv, Throwable, Unit] => ZIO[RunEnv, Throwable, Unit] =
    job =>
      for {
        spark <- ZIO.access[SparkEnv](_.get.sparkEnv)
        result <- job
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

  /**
   * Runs the spark job to load data into JanusGraph
   */
  val runGrafinkJob: JobTime => ZIO[RunEnv, Throwable, Unit] = jobTime => runJob(process(jobTime))

  /**
   * Runs the spark job to delete data present in IDManager storage
   */
  val runGrafinkDeleteJob: JobTime => ZIO[RunEnv, Throwable, Unit] = jobTime => runJob(delete(jobTime))
}
