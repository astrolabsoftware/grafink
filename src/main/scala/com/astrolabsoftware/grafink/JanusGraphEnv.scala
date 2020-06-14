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

import org.janusgraph.core.{ JanusGraph, JanusGraphFactory }
import org.janusgraph.diskstorage.util.time.TimestampProviders
import zio.{ Has, ZLayer }
import zio.blocking.Blocking

import com.astrolabsoftware.grafink.models.JanusGraphConfig

object JanusGraphEnv {

  type JanusGraphEnv = Has[JanusGraphEnv.Service]

  trait Service {
    val graph: JanusGraph
  }

  def make(
    graph: JanusGraphConfig => JanusGraph
  ): ZLayer[Blocking with Has[JanusGraphConfig], Throwable, Has[Service]] =
    ZLayer.fromFunctionManyM { blockingWithConfig =>
      blockingWithConfig.get
        .effectBlocking(graph)
        .map { jGraph =>
          Has(new Service {
            override val graph: JanusGraph = jGraph(blockingWithConfig.get[JanusGraphConfig])
          })
        }
    }

  def hbaseBasic(): ZLayer[Blocking with Has[JanusGraphConfig], Throwable, Has[Service]] =
    make(config => withHBaseStorage(config))

  def hbase(): ZLayer[Blocking with Has[JanusGraphConfig], Throwable, Has[Service]] =
    make(config => withHBaseStorageWithBulkLoad(config))

  def inmemory(): ZLayer[Blocking with Has[JanusGraphConfig], Throwable, Has[Service]] =
    make(inMemoryStorage(_))

  def inMemoryStorage: JanusGraphConfig => JanusGraph =
    config =>
      JanusGraphFactory.build
      // Use hbase as storage backend
        .set("storage.backend", "inmemory")
        .set("schema.default", "none")
        // Manual transactions
        .set("storage.transactions", false)
        // Allow setting vertex ids
        .set("graph.set-vertex-id", true)
        .open()

  def withHBaseStorage: JanusGraphConfig => JanusGraph =
    config =>
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
        // Allow setting vertex ids
        .set("graph.set-vertex-id", true)
        .open()

  def withHBaseStorageWithBulkLoad: JanusGraphConfig => JanusGraph =
    config =>
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
        .open()
}
