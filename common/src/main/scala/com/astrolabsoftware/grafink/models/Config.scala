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
import zio._
import zio.logging.{log, Logging}

case class RenameColumn(f: String, t: String)

case class NewColumn(name: String, expr: String)

case class ReaderConfig(basePath: String, format: Format, keepCols: List[String], keepColsRenamed: List[RenameColumn], newCols: List[NewColumn] = List.empty)

case class HBaseZookeeperConfig(quoram: String)

case class HBaseConfig(zookeeper: HBaseZookeeperConfig)

case class PropertySchema(name: String, typ: String)

case class VertexLabelConfig(name: String, properties: List[PropertySchema], propertiesFromData: List[String])

case class EdgeLabelConfig(name: String, properties: List[PropertySchema])

case class CompositeIndex(name: String, properties: List[String])

case class MixedIndex(name: String, properties: List[String])

case class EdgeIndex(name: String, properties: List[String], label: String)

case class IndexConfig(composite: List[CompositeIndex], mixed: List[MixedIndex], edge: List[EdgeIndex])

case class SchemaConfig(
  vertexLabels: List[VertexLabelConfig],
  // vertexPropertyCols: List[String],
  // vertexLabel: String,
  edgeLabels: List[EdgeLabelConfig],
  index: IndexConfig
)

case class FixedVertexProperty(name: String, datatype: String, value: AnyRef)

case class FixedVertex(id: Long, label: String, properties: List[FixedVertexProperty])

case class VertexLoaderConfig(batchSize: Int, label: String, fixedVertices: String)

case class SimilarityConfig(similarityExp: String)

case class TwoModeSimilarityConfig(recipes: List[String])

case class SameValueSimilarityConfig(colsToConnect: List[String])

case class EdgeRulesConfig(similarityClassifer: SimilarityConfig, twoModeClassifier: TwoModeSimilarityConfig, sameValueConfig: SameValueSimilarityConfig)

case class EdgeLoaderConfig(batchSize: Int, parallelism: Int, taskSize: Int, rulesToApply: List[String], rules: EdgeRulesConfig)

case class JanusGraphStorageConfig(host: String, port: Int, tableName: String, extraConf: List[String])

case class JanusGraphIndexBackendConfig(name: String, indexName: String, host: String)

case class GrafinkJobConfig(
   schema: SchemaConfig,
   vertexLoader: VertexLoaderConfig,
   edgeLoader: EdgeLoaderConfig
)

case class JanusGraphConfig(
  storage: JanusGraphStorageConfig,
  indexBackend: JanusGraphIndexBackendConfig
)

case class GrafinkJanusGraphConfig(
  job: GrafinkJobConfig,
  janusGraph: JanusGraphConfig
)

case class IDManagerSparkConfig(reservedIdSpace: Int, dataPath: String, clearOnDelete: Boolean)

case class HBaseColumnConfig(tableName: String, cf: String, qualifier: String)

case class IDManagerConfig(spark: IDManagerSparkConfig, hbase: HBaseColumnConfig)

final case class GrafinkConfiguration(
  reader: ReaderConfig,
  hbase: HBaseConfig,
  job: GrafinkJobConfig,
  janusgraph: JanusGraphConfig,
  idManager: IDManagerConfig
)

package object config {

  // For pure config to be able to use camel case
  implicit def hint[T]: ProductHint[T]          = ProductHint[T](ConfigFieldMapping(CamelCase, CamelCase))
  implicit val formatHint: ConfigReader[Format] = deriveEnumerationReader[Format]

  type GrafinkConfig = Has[ReaderConfig] with Has[HBaseConfig] with Has[GrafinkJobConfig] with Has[JanusGraphConfig] with Has[IDManagerConfig]

  object Config {

    val live: (String) => ZLayer[Logging, Throwable, GrafinkConfig] = confFile =>
      ZLayer.fromEffectMany(
        Task
          .effect(ConfigSource.file(confFile).loadOrThrow[GrafinkConfiguration])
          .tapError(throwable => log.error(s"Loading configuration failed with error: $throwable"))
          .map(c => Has(c.reader) ++ Has(c.hbase) ++ Has(c.job) ++ Has(c.janusgraph) ++ Has(c.idManager))
      )

    val readerConfig: URIO[Has[ReaderConfig], ReaderConfig]             = ZIO.service
    val hbaseConfig: URIO[Has[HBaseConfig], HBaseConfig]                = ZIO.service
    val jobConfig: URIO[Has[GrafinkJobConfig], GrafinkJobConfig]        = ZIO.service
    val janusGraphConfig: URIO[Has[JanusGraphConfig], JanusGraphConfig] = ZIO.service
    val idManagerConfig: URIO[Has[IDManagerConfig], IDManagerConfig]    = ZIO.service
    val grafinkJanusGraphConfig: URIO[Has[GrafinkJobConfig] with Has[JanusGraphConfig], GrafinkJanusGraphConfig] =
      for {
        jobConf <- jobConfig
        janusGraphConf <- janusGraphConfig
      } yield GrafinkJanusGraphConfig(jobConf, janusGraphConf)
  }
}
