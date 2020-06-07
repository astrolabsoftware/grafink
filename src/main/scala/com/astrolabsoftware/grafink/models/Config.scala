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

import pureconfig._
import pureconfig.generic.ProductHint
import pureconfig.generic.auto._
import pureconfig.generic.semiauto._
import zio.{Has, Task, URIO, ZIO, ZLayer}
import zio.logging.{log, Logging}

case class ReaderConfig(basePath: String, format: Format)

case class HBaseZookeeperConfig(quoram: String)

case class HBaseConfig(zookeeper: HBaseZookeeperConfig)

case class JanusGraphConfig(tableName: String)

case class SparkPathConfig(dataPath: String)

case class HBaseColumnConfig(tableName: String, cf: String, qualifier: String)

case class IDManagerConfig(spark: SparkPathConfig, hbase: HBaseColumnConfig)

final case class GrafinkConfiguration(
  reader: ReaderConfig,
  hbase: HBaseConfig,
  janusgraph: JanusGraphConfig,
  idManager: IDManagerConfig
)

package object config {

  // For pure config to be able to use camel case
  implicit def hint[T]: ProductHint[T]          = ProductHint[T](ConfigFieldMapping(CamelCase, CamelCase))
  implicit val formatHint: ConfigReader[Format] = deriveEnumerationReader[Format]

  type GrafinkConfig = Has[ReaderConfig] with Has[HBaseConfig] with Has[JanusGraphConfig] with Has[IDManagerConfig]

  object Config {

    val live: (String) => ZLayer[Logging, Throwable, GrafinkConfig] = confFile =>
      ZLayer.fromEffectMany(
        Task
          .effect(ConfigSource.file(confFile).loadOrThrow[GrafinkConfiguration])
          .tapError(throwable => log.error(s"Loading configuration failed with error: $throwable"))
          .map(c => Has(c.reader) ++ Has(c.hbase) ++ Has(c.janusgraph) ++ Has(c.idManager))
      )

    val readerConfig: URIO[Has[ReaderConfig], ReaderConfig]             = ZIO.service
    val hbaseConfig: URIO[Has[HBaseConfig], HBaseConfig]                = ZIO.service
    val janusGraphConfig: URIO[Has[JanusGraphConfig], JanusGraphConfig] = ZIO.service
    val idManagerConfig: URIO[Has[IDManagerConfig], IDManagerConfig]    = ZIO.service
  }
}
