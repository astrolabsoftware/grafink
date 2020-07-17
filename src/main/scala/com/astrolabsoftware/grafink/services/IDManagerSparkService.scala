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
package com.astrolabsoftware.grafink.services

import org.apache.spark.sql.{ DataFrame, Row, SaveMode, SparkExtensions, SparkSession }
import org.apache.spark.sql.functions.{ col, max }
import org.apache.spark.sql.types.{ LongType, StructField, StructType }
import zio._
import zio.logging.{ log, Logging }

import com.astrolabsoftware.grafink.Job.{ SparkEnv, VertexData }
import com.astrolabsoftware.grafink.common.PartitionManager
import com.astrolabsoftware.grafink.models.GrafinkException.GetIdException
import com.astrolabsoftware.grafink.models.IDManagerConfig
import com.astrolabsoftware.grafink.models.config.Config
import com.astrolabsoftware.grafink.services.IDManager.IDType

/**
 * An id manager service using spark.
 * This will store the data along with generated ids at
 * a configured location, per hbase table.
 * To generate new ids, we take the max id from existing data
 * and continue generating new ids incrementally from it.
 */
object IDManagerSparkService {

  type IDType                = Long
  type IDManagerSparkService = Has[IDManagerSparkService.Service]

  trait Service {

    /**
     * Reads all the intermediate data which is already ingested in janusgraph so far
     * This means all rows have an "id" column which was generated when this data was
     * processed
     * @return
     */
    def readAll(schema: StructType, tableName: String): ZIO[Logging, Throwable, DataFrame]

    /**
     * Processes the input data for this job to generate and add new ids, then saves this data
     * @param df
     * @param tableName
     * @return
     */
    def process(df: DataFrame, tableName: String): ZIO[Logging, Throwable, VertexData]
    def fetchID(df: DataFrame): RIO[Logging, IDType]
  }

  val live: URLayer[Logging with SparkEnv with Has[IDManagerConfig], IDManagerSparkService] =
    ZLayer.fromEffect(
      for {
        spark  <- ZIO.access[SparkEnv](_.get.sparkEnv)
        config <- Config.idManagerConfig
      } yield new IDManagerSparkServiceLive(spark, config)
    )

  def readAll(schema: StructType, tableName: String): RIO[IDManagerSparkService with Logging, DataFrame] =
    RIO.accessM(_.get.readAll(schema, tableName))

  def fetchID(df: DataFrame): RIO[IDManagerSparkService with Logging, IDType] =
    RIO.accessM(_.get.fetchID(df))

  def process(df: DataFrame, tableName: String): ZIO[IDManagerSparkService with Logging, Throwable, VertexData] =
    RIO.accessM(_.get.process(df, tableName))
}

final class IDManagerSparkServiceLive(spark: SparkSession, config: IDManagerConfig)
    extends IDManagerSparkService.Service {

  def addId(df: DataFrame, lastMax: IDType): DataFrame =
    SparkExtensions.zipWithIndex(df, lastMax + 1)

  override def readAll(schema: StructType, tableName: String): ZIO[Logging, Throwable, DataFrame] =
    ZIO.effect(spark.read.parquet(s"${config.spark.dataPath}/$tableName")).catchSome {
      // Catch case where there is no data to read, this means this is being run on a new setup
      case e: org.apache.spark.sql.AnalysisException
          if e.message.contains("Unable to infer schema for Parquet") ||
            e.message.contains("Path does not exist") =>
        for {
          _ <- log.warn(s"No data found at ${config.spark.dataPath}/$tableName, returning empty dataframe")
        } yield spark.createDataFrame(
          spark.sparkContext.emptyRDD[Row],
          StructType(StructField("id", LongType, false) +: schema.fields)
        )
    }

  override def process(df: DataFrame, tableName: String): ZIO[Logging, Throwable, VertexData] =
    for {
      // All the idmanager data so far ingested
      idManagerDf <- readAll(df.schema, tableName)
      // Get the last max id used
      lastMax <- fetchID(idManagerDf)
      // Generate new ids for the `jobTime` data
      dfWithId <- processData(lastMax, df, tableName)
    } yield VertexData(loaded = idManagerDf, current = dfWithId)

  def processData(id: IDType, df: DataFrame, tableName: String): ZIO[Logging, Throwable, DataFrame] = {

    val mode = SaveMode.Append

    val dfWithId = addId(df, id)

    for {
      // Cache this df
      dfWithIdCached <- ZIO.effect(dfWithId.cache)
      // Write intermediate data
      _ <- ZIO.effect(
        dfWithIdCached.write
          .format("parquet")
          .mode(mode)
          .partitionBy(PartitionManager.partitionColumns: _*)
          .save(s"${config.spark.dataPath}/$tableName")
      )
    } yield dfWithIdCached
  }

  override def fetchID(df: DataFrame): RIO[Logging, IDType] =
    // Get the highest id from data in IDManagerConfig.SparkPathConfig.dataPath
    // Simplest way is to read whole data and get the max
    // TODO: Modify this to read data from only the latest day's data present in the path
    for {
      res <- ZIO.effect(df.select(max(col("id"))).collect)
      currentID = res.headOption.map(r => if (r.isNullAt(0)) 0L else r.getLong(0))
      _ <- if (currentID.isDefined) log.info(s"Returning current max id = ${currentID.get}")
      else ZIO.fail(GetIdException(s"Did not get valid max id"))
    } yield currentID.get
}
