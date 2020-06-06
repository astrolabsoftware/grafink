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

import zio._
import zio.logging.Logging

import com.astrolabsoftware.grafink.Job.{ JobTime, SparkEnv }
import com.astrolabsoftware.grafink.common.PartitionManager
import com.astrolabsoftware.grafink.hbase.HBaseClientService
import com.astrolabsoftware.grafink.hbase.HBaseClientService.HBaseClientService
import com.astrolabsoftware.grafink.models.{ IDManagerConfig, JanusGraphConfig }
import com.astrolabsoftware.grafink.models.config.Config
import com.astrolabsoftware.grafink.services.IDManager.IDType

object IDManager {

  type IDType           = Long
  type IDManagerService = Has[IDManager.Service]

  trait Service {
    def config: IDManagerConfig
    def fetchID(jobTime: JobTime): RIO[Logging, Option[IDType]]
  }

  val liveUHbase
    : URLayer[Logging with HBaseClientService with Has[JanusGraphConfig] with Has[IDManagerConfig], IDManagerService] =
    ZLayer.fromEffect(
      for {
        client      <- ZIO.service[HBaseClientService.Service]
        janusConfig <- Config.janusGraphConfig
        idConfig    <- Config.idManagerConfig
      } yield new IDManagerHBaseService(client, idConfig, janusConfig)
    )

  val liveUSpark: URLayer[Logging with SparkEnv with Has[IDManagerConfig], IDManagerService] =
    ZLayer.fromEffect(
      for {
        spark  <- ZIO.access[SparkEnv](_.get.sparkEnv)
        config <- Config.idManagerConfig
      } yield new IDManagerSparkService(spark, config)
    )

  def fetchID(jobTime: JobTime): RIO[IDManagerService with Logging, Option[IDType]] =
    RIO.accessM(_.get.fetchID(jobTime))
}

final class IDManagerHBaseService(
  client: HBaseClientService.Service,
  idConfig: IDManagerConfig,
  janusConfig: JanusGraphConfig
) extends IDManager.Service {

  override val config: IDManagerConfig = idConfig

  override def fetchID(jobTime: JobTime): RIO[Logging, Option[IDType]] = {
    val key = makeIdKey(s"${jobTime.day.format(PartitionManager.dateFormat)}", janusConfig.tableName)
    for {
      res <- client.getFromTable(key, config.hbase.cf, config.hbase.qualifier, config.hbase.tableName)
      id = res.map(_.toLong)
    } yield id
  }

  def makeIdKey(day: String, tableName: String): String = s"$day-$tableName"
}
