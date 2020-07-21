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

import org.janusgraph.core.{JanusGraph, JanusGraphFactory}
import org.janusgraph.diskstorage.util.time.TimestampProviders
import zio.{ZIO, ZManaged}
import zio.logging.{log, Logging}

import com.astrolabsoftware.grafink.StringUtils._
import com.astrolabsoftware.grafink.models.JanusGraphConfig

object JanusGraphEnv extends Serializable {

  def releaseGraph: JanusGraph => zio.URIO[Any, Unit] =
    (graph: JanusGraph) =>
      (for {
        _ <- ZIO.effect(graph.tx().commit())
        _ <- ZIO.effect(graph.close())
      } yield ())
        .fold(_ => log.error(s"Error closing janusgraph instance"), _ => log.info(s"JanusGraph instance closed"))

  def hbaseBasic(config: JanusGraphConfig): ZManaged[Logging, Throwable, JanusGraph] =
    ZIO
      .effect(withHBaseStorage(config))
      .toManaged(releaseGraph)

  def hbase(config: JanusGraphConfig): ZManaged[Logging, Throwable, JanusGraph] =
    ZIO
      .effect(withHBaseStorageWithBulkLoad(config))
      .toManaged(releaseGraph)

  def inmemory(config: JanusGraphConfig): ZManaged[Logging, Throwable, JanusGraph] =
    ZIO
      .effect(inMemoryStorage(config))
      .toManaged(releaseGraph)

  def inMemoryStorage: JanusGraphConfig => JanusGraph =
    config =>
      JanusGraphFactory.build
      // Use hbase as storage backend
        .set("storage.backend", "inmemory")
        // Manual transactions
        .set("storage.transactions", false)
        // Allow setting vertex ids
        .set("graph.set-vertex-id", true)
        .open()

  def withHBaseStorage: JanusGraphConfig => JanusGraph = { config =>
    val builder =
      withExtraConf(
        config,
        JanusGraphFactory.build
        // Use hbase as storage backend
          .set("storage.backend", "hbase")
          .set("graph.timestamps", TimestampProviders.MILLI)
          // Configure hbase as storage backend
          .set("storage.hostname", config.storage.host)
          // Use the configured table name
          .set("storage.hbase.table", config.storage.tableName)
          // Manual transactions
          .set("storage.transactions", false)
          // Allow setting vertex ids
          .set("graph.set-vertex-id", true)
      )
    if (config.schema.index.mixed.nonEmpty) {
      withIndexingBackend(config, builder).open()
    } else {
      builder.open()
    }
  }

  def withHBaseStorageWithBulkLoad: JanusGraphConfig => JanusGraph = { config =>
    val builder =
      withExtraConf(
        config,
        JanusGraphFactory.build
        // Use hbase as storage backend
          .set("storage.backend", "hbase")
          .set("graph.timestamps", TimestampProviders.MILLI)
          // Configure hbase as storage backend
          .set("storage.hostname", config.storage.host)
          // Use the configured table name
          .set("storage.hbase.table", config.storage.tableName)
          .set("schema.default", "none")
          // Manual transactions
          .set("storage.transactions", false)
          // Use batch loading
          .set("storage.batch-loading", true)
          // Allow setting vertex ids
          .set("graph.set-vertex-id", true)
      )
    if (config.schema.index.mixed.nonEmpty) {
      withIndexingBackend(config, builder).open()
    } else {
      builder.open()
    }
  }

  def withExtraConf(config: JanusGraphConfig, builder: JanusGraphFactory.Builder): JanusGraphFactory.Builder = {
    val extraConf = config.storage.extraConf.map(c => s"storage.hbase.ext.$c")
    extraConf.foldLeft(builder) { (b, c) =>
      val (k, v) = c.splitToTuple("=")
      b.set(k, v)
    }
  }

  def withIndexingBackend(config: JanusGraphConfig, builder: JanusGraphFactory.Builder): JanusGraphFactory.Builder = {
    // Set Indexing backend option, since running with mixed indices
    val backend     = config.indexBackend
    val backendName = backend.name
    builder
      .set(s"index.$backendName.backend", "elasticsearch")
      .set(s"index.$backendName.hostname", backend.host)
      .set(s"index.$backendName.index-name", backend.indexName)
      .set(s"index.$backendName.elasticsearch.bulk-refresh", "true")
  }

  def withGraph[R](config: JanusGraphConfig, use: JanusGraph => ZIO[R, Throwable, Unit]): ZIO[R, Throwable, Unit] =
    ZIO.effect(withHBaseStorageWithBulkLoad(config)).bracket(releaseGraph, use)
}
