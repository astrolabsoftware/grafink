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

import com.astrolabsoftware.grafink.CLParser
import com.astrolabsoftware.grafink.Job.JobTime
import com.astrolabsoftware.grafink.hbase.HBaseClientService
import com.astrolabsoftware.grafink.hbase.HBaseClientService.HBaseClientService
import com.astrolabsoftware.grafink.models.IDManagerConfig
import com.astrolabsoftware.grafink.models.config.Config
import com.astrolabsoftware.grafink.services.IDManager.IDType

object IDManager {

  type IDType           = Long
  type IDManagerService = Has[IDManager.Service]

  trait Service {
    def config: IDManagerConfig
    def fetchID(jobTime: JobTime): RIO[Logging, Option[IDType]]
  }

  val live: URLayer[Logging with HBaseClientService with Has[IDManagerConfig], IDManagerService] =
    ZLayer.fromEffect(
      for {
        client <- ZIO.service[HBaseClientService.Service]
        config <- Config.idManagerConfig
      } yield new IDManagerServiceLive(client, config)
    )

  def makeIdKey(day: String, tableName: String): String = s"$day-$tableName"
  def fetchID(jobTime: JobTime): RIO[IDManagerService with Logging, Option[IDType]] =
    RIO.accessM(_.get.fetchID(jobTime))
}

final class IDManagerServiceLive(client: HBaseClientService.Service, _config: IDManagerConfig)
    extends IDManager.Service {

  override val config: IDManagerConfig = _config

  override def fetchID(jobTime: JobTime): RIO[Logging, Option[IDType]] = {
    val key = IDManager.makeIdKey(s"${jobTime.day.format(CLParser.dateFormat)}", config.tableName)
    for {
      res <- client.getFromTable(key, config.cf, config.qualifier, config.tableName)
      id = res.map(_.toLong)
    } yield id
  }
}
