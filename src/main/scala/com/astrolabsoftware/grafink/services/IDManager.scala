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
import com.astrolabsoftware.grafink.models.GrafinkException.GetIdException
import com.astrolabsoftware.grafink.models.config.Config
import com.astrolabsoftware.grafink.services.IDManager.IDType

object IDManager {

  type IDType           = Long
  type IDManagerService = Has[IDManager.Service]

  trait Service {
    def fetchID(jobTime: JobTime): RIO[Logging, IDType]
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

  def fetchID(jobTime: JobTime): RIO[IDManagerService with Logging, IDType] =
    RIO.accessM(_.get.fetchID(jobTime))
}

final class IDManagerHBaseService(
  client: HBaseClientService.Service,
  idConfig: IDManagerConfig,
  janusConfig: JanusGraphConfig
) extends IDManager.Service {

  override def fetchID(jobTime: JobTime): RIO[Logging, IDType] = {
    val key = makeIdKey(s"${jobTime.day.format(PartitionManager.dateFormat)}", janusConfig.storage.tableName)
    for {
      res <- client.getFromTable(key, idConfig.hbase.cf, idConfig.hbase.qualifier, idConfig.hbase.tableName)
      _ = if (res.isEmpty)
        ZIO.fail(GetIdException(s"Error getting validId from hbase table ${idConfig.hbase.tableName}"))
      id = res.get.toLong
    } yield id
  }

  def makeIdKey(day: String, tableName: String): String = s"$day-$tableName"
}
