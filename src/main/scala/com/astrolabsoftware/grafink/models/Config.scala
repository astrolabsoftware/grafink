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
package com.astrolabsoftware.grafink.models

import pureconfig.{ CamelCase, ConfigFieldMapping, ConfigSource }
import pureconfig.generic.ProductHint
import pureconfig.generic.auto._
import zio.{ Has, Task, URIO, ZIO, ZLayer }
import zio.logging.{ log, Logging }

case class ReaderConfig(basePath: String)

case class HBaseZookeeperConfig(quoram: String)

case class HBaseConfig(zookeeper: HBaseZookeeperConfig)

case class IDManagerConfig(tableName: String, cf: String, qualifier: String)

final case class GrafinkConfiguration(reader: ReaderConfig, hbase: HBaseConfig, idManager: IDManagerConfig)

package object config {

  // For pure config to be able to use camel case
  implicit def hint[T]: ProductHint[T] = ProductHint[T](ConfigFieldMapping(CamelCase, CamelCase))

  type GrafinkConfig = Has[ReaderConfig] with Has[HBaseConfig] with Has[IDManagerConfig]

  object Config {

    val live: (String) => ZLayer[Logging, Throwable, GrafinkConfig] = confFile =>
      ZLayer.fromEffectMany(
        Task
          .effect(ConfigSource.file(confFile).loadOrThrow[GrafinkConfiguration])
          .tapError(throwable => log.error(s"Loading configuration failed with error: $throwable"))
          .map(c => Has(c.reader) ++ Has(c.hbase) ++ Has(c.idManager))
      )

    val readerConfig: URIO[Has[ReaderConfig], ReaderConfig]          = ZIO.service
    val hbaseConfig: URIO[Has[HBaseConfig], HBaseConfig]             = ZIO.service
    val idManagerConfig: URIO[Has[IDManagerConfig], IDManagerConfig] = ZIO.service
  }
}
