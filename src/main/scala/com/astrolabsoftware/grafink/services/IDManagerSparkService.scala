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

import org.apache.spark.sql.SparkSession
import zio.{ RIO, ZIO }
import zio.logging.{ log, Logging }

import com.astrolabsoftware.grafink.Job.JobTime
import com.astrolabsoftware.grafink.models.GrafinkException.GetIdException
import com.astrolabsoftware.grafink.models.IDManagerConfig
import com.astrolabsoftware.grafink.services.IDManager.IDType

final class IDManagerSparkService(spark: SparkSession, _config: IDManagerConfig) extends IDManager.Service {

  override val config: IDManagerConfig = _config

  override def fetchID(jobTime: JobTime): RIO[Logging, IDType] = {
    // Get the highest id from data in IDManagerConfig.SparkPathConfig.dataPath
    // Simplest way is to read whole data and get the max
    // TODO: Modify this to read data from only the latest day's data present in the path
    import org.apache.spark.sql.functions.{ col, max }
    val computeId =
      for {
        df  <- ZIO.effect(spark.read.parquet(config.spark.dataPath))
        res <- ZIO.effect(df.select(max(col("id"))).collect)
        _ = if (res.isEmpty) {
          ZIO.fail(GetIdException(s"Error getting valid id from ${config.spark.dataPath}, max returned empty result"))
        }
        currentID = res.headOption.map(_.getLong(0))
        _ <- if (currentID.isDefined) log.info(s"Returning current max id = ${currentID.get}")
        else log.warn(s"Did not get valid max id")
      } yield currentID.get

    computeId.catchSome {
      // Catch case where there is no data to read, this means this is being run on a new setup
      case e: org.apache.spark.sql.AnalysisException if e.message.contains("Unable to infer schema for Parquet") =>
        for {
          _ <- log.warn(s"No data found at ${config.spark.dataPath}, returning -1")
        } yield -1L
    }
  }
}
