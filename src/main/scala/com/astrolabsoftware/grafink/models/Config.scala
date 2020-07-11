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
import zio.{ Has, Task, URIO, ZIO, ZLayer }
import zio.logging.{ log, Logging }

case class RenameColumn(f: String, t: String)

case class ReaderConfig(basePath: String, format: Format, keepCols: List[String], keepColsRenamed: List[RenameColumn])

case class HBaseZookeeperConfig(quoram: String)

case class HBaseConfig(zookeeper: HBaseZookeeperConfig)

case class EdgeLabelConfig(name: String, properties: Map[String, String])

case class CompositeIndex(name: String, properties: List[String])

case class MixedIndex(name: String, properties: List[String])

case class EdgeIndex(name: String, properties: List[String], label: String)

case class IndexConfig(composite: List[CompositeIndex], mixed: List[MixedIndex], edge: List[EdgeIndex])

case class SchemaConfig(
  vertexPropertyCols: List[String],
  vertexLabel: String,
  edgeLabels: List[EdgeLabelConfig],
  index: IndexConfig
)

case class VertexLoaderConfig(batchSize: Int)

case class SimilarityConfig(similarityExp: String)

case class EdgeRulesConfig(similarityClassifer: SimilarityConfig)

case class EdgeLoaderConfig(batchSize: Int, parallelism: Int, taskSize: Int, rules: EdgeRulesConfig)

case class JanusGraphStorageConfig(host: String, port: Int, tableName: String)

case class JanusGraphIndexBackendConfig(name: String, indexName: String, host: String)

case class JanusGraphConfig(
  schema: SchemaConfig,
  vertexLoader: VertexLoaderConfig,
  edgeLoader: EdgeLoaderConfig,
  storage: JanusGraphStorageConfig,
  indexBackend: JanusGraphIndexBackendConfig
)

case class IDManagerSparkConfig(dataPath: String, clearOnDelete: Boolean)

case class HBaseColumnConfig(tableName: String, cf: String, qualifier: String)

case class IDManagerConfig(spark: IDManagerSparkConfig, hbase: HBaseColumnConfig)

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
