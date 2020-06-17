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
package com.astrolabsoftware.grafink.processor

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.{GraphTraversal, GraphTraversalSource}
import org.apache.tinkerpop.gremlin.structure.Vertex
import org.janusgraph.graphdb.database.StandardJanusGraph
import zio._
import zio.blocking.Blocking
import zio.logging.{log, Logging}

import com.astrolabsoftware.grafink.JanusGraphEnv
import com.astrolabsoftware.grafink.JanusGraphEnv.JanusGraphEnv
import com.astrolabsoftware.grafink.Job.{SparkEnv, VertexData}
import com.astrolabsoftware.grafink.models.JanusGraphConfig
import com.astrolabsoftware.grafink.models.config.Config
import com.astrolabsoftware.grafink.processor.EdgeProcessor.MakeEdge

object EdgeProcessor {

  // TODO Support edge properties
  case class MakeEdge(src: Long, dst: Long, label: String)

  type EdgeProcessorService = Has[EdgeProcessor.Service]

  trait Service {
    def process(vertexData: VertexData, rules: List[VertexClassifierRule]): ZIO[Logging, Throwable, Unit]
    def loadEdges(edgesRDD: Dataset[MakeEdge]): ZIO[Logging, Throwable, Unit]
  }

  val live: URLayer[SparkEnv with Has[JanusGraphConfig] with Logging, EdgeProcessorService] =
    ZLayer.fromEffect(
      for {
        spark            <- ZIO.access[SparkEnv](_.get.sparkEnv)
        janusGraphConfig <- Config.janusGraphConfig
      } yield new EdgeProcessorLive(spark, janusGraphConfig)
    )

  def process(vertexData: VertexData, rules: List[VertexClassifierRule]): ZIO[EdgeProcessorService with Logging, Throwable, Unit] =
    ZIO.accessM(_.get.process(vertexData, rules))
}

final class EdgeProcessorLive(spark: SparkSession, config: JanusGraphConfig) extends EdgeProcessor.Service {

  override def process(vertexData: VertexData, rules: List[VertexClassifierRule]): ZIO[Logging, Throwable, Unit] =
    for {
      _ <- ZIO.collectAll(rules.map { rule =>
          for {
          _ <- log.info(s"Adding edges using rule ${rule.name}")
          _ <- loadEdges(rule.classify(vertexData.loaded, vertexData.current))
          } yield ()
        })
    } yield()

  override def loadEdges(edgesRDD: Dataset[MakeEdge]): ZIO[Logging, Throwable, Unit] = {

    val batchSize        = config.edgeLoader.batchSize
    val c = config

    // Vertex cache
    val vCache = ZRef.make(Map.empty[Long, GraphTraversal[Vertex, Vertex]])
    // Add edges to all rows within a partition

    def loaderFunc: (Iterator[MakeEdge]) => Unit = (partition: Iterator[MakeEdge]) => {

      val janusGraphLayer = (Blocking.live ++ ZLayer.succeed(c)) >>> JanusGraphEnv.hbase()

      @inline
      def getFromCacheOrGraph(g: GraphTraversalSource, id: Long): ZIO[Any, Throwable, GraphTraversal[Vertex, Vertex]] =
        for {
          cacheRef <- vCache
          cache <- cacheRef.get
          vertex <- ZIO.fromOption(cache.get(id)).orElse(
            for {
              v <- ZIO.effect(g.V(java.lang.Long.valueOf(id)))
              _ <- cacheRef.update(_.updated(id, v))
            } yield v
          )
        } yield vertex

      val job =
        for {
          graph <- ZIO.access[JanusGraphEnv](_.get.graph)
          g = graph.traversal()
          idManager = graph.asInstanceOf[StandardJanusGraph].getIDManager
          kgroup    = partition.grouped(batchSize)
          l = kgroup.map(group =>
            for {
              _ <- ZIO.collectAll_(
                group.map { r =>
                  for {
                    // Safe to get here since we know its already loaded
                    srcVertex <- getFromCacheOrGraph(g, idManager.toVertexId(r.src))
                    dstVertex <- getFromCacheOrGraph(g, idManager.toVertexId(r.dst))
                    // TODO: Derive the labels from schema config by extending MakeEdge class
                    _ <- ZIO.effect(srcVertex.addE( "similarity").to(dstVertex).property("value", r.label).iterate)
                    // Add reverse edge as well
                    _ <- ZIO.effect(dstVertex.addE("similarity").to(srcVertex).property("value", r.label).iterate)
                  } yield ()
                }
              )
              _ <- ZIO.effect(g.tx.commit)
            } yield ()
          )
          _ <- ZIO.collectAll_(l.toIterable)
          // Additional commit if anything left
          _ <- ZIO.effect(g.tx.commit)
          // Make this managed
          _ <- ZIO.effect(g.close)
        } yield ()
      zio.Runtime.default.unsafeRun(job.provideLayer(janusGraphLayer))
    }

    val load = loaderFunc

    ZIO.effect(spark.sparkContext.runJob(edgesRDD.rdd, load))
  }
}
