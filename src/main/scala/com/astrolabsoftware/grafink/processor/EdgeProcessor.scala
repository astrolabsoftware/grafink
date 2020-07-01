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

import org.apache.spark.HashPartitioner
import org.apache.spark.sql.{ Dataset, SparkSession }
import org.janusgraph.core.JanusGraph
import org.janusgraph.graphdb.database.StandardJanusGraph
import zio._
import zio.logging.{ log, Logging }

import com.astrolabsoftware.grafink.JanusGraphEnv.withGraph
import com.astrolabsoftware.grafink.Job.{ SparkEnv, VertexData }
import com.astrolabsoftware.grafink.models.JanusGraphConfig
import com.astrolabsoftware.grafink.models.config.Config
import com.astrolabsoftware.grafink.processor.EdgeProcessor.{ EdgeStats, MakeEdge }
import com.astrolabsoftware.grafink.processor.edgerules.VertexClassifierRule

/**
 * Interface that loads the edges data into JanusGraph
 */
object EdgeProcessor {

  // TODO Support proper data type for edge properties, and support multiple properties
  case class MakeEdge(src: Long, dst: Long, propVal: Int)
  case class EdgeStats(count: Int, partitions: Int)

  type EdgeProcessorService = Has[EdgeProcessor.Service]

  trait Service {
    def process(vertexData: VertexData, rules: List[VertexClassifierRule]): ZIO[Logging, Throwable, Unit]
    def loadEdges(edgesRDD: Dataset[MakeEdge], label: String): ZIO[Logging, Throwable, Unit]
  }

  /**
   * Get the EdgeProcessLive instance
   */
  val live: URLayer[Has[JanusGraphConfig] with Logging, EdgeProcessorService] =
    ZLayer.fromEffect(
      for {
        janusGraphConfig <- Config.janusGraphConfig
      } yield EdgeProcessorLive(janusGraphConfig)
    )

  /**
   * Given a collection of rules, adds a set of edges per rule to the graph
   * @param vertexData
   * @param rules
   * @return
   */
  def process(
    vertexData: VertexData,
    rules: List[VertexClassifierRule]
  ): ZIO[EdgeProcessorService with Logging, Throwable, Unit] =
    ZIO.accessM(_.get.process(vertexData, rules))
}

/**
 * Loads edge data into JanusGraph
 * @param config
 */
final case class EdgeProcessorLive(config: JanusGraphConfig) extends EdgeProcessor.Service {

  override def process(vertexData: VertexData, rules: List[VertexClassifierRule]): ZIO[Logging, Throwable, Unit] =
    for {
      _ <- ZIO.collectAll(rules.map { rule =>
        for {
          _ <- log.info(s"Adding edges using rule ${rule.name}")
          _ <- loadEdges(rule.classify(vertexData.loaded, vertexData.current), rule.getEdgeLabel)
        } yield ()
      })
    } yield ()

  def job(
    config: JanusGraphConfig,
    graph: JanusGraph,
    label: String,
    partition: Iterator[MakeEdge]
  ): ZIO[Any, Throwable, Unit] = {
    val batchSize = config.edgeLoader.batchSize
    val g         = graph.traversal()
    val idManager = graph.asInstanceOf[StandardJanusGraph].getIDManager
    val kgroup    = partition.grouped(batchSize)
    val l = kgroup.map(group =>
      for {
        _ <- ZIO.collectAll_(
          group.map {
            r =>
              // TODO: Optimize
              for {
                // Safe to get here since we know its already loaded
                srcVertex <- ZIO.effect(g.V(java.lang.Long.valueOf(idManager.toVertexId(r.src))))
                dstVertex <- ZIO.effect(g.V(java.lang.Long.valueOf(idManager.toVertexId(r.dst))))
                _         <- ZIO.effect(srcVertex.addE(label).to(dstVertex).property("value", r.propVal).iterate)
                // Add reverse edge as well
                srcVertex <- ZIO.effect(g.V(java.lang.Long.valueOf(idManager.toVertexId(r.src))))
                dstVertex <- ZIO.effect(g.V(java.lang.Long.valueOf(idManager.toVertexId(r.dst))))
                _         <- ZIO.effect(dstVertex.addE(label).to(srcVertex).property("value", r.propVal).iterate)
              } yield ()
          }
        )
        _ <- ZIO.effect(g.tx.commit)
      } yield ()
    )
    for {
      _ <- ZIO.collectAll_(l.toIterable)
      // Additional commit if anything left
      _ <- ZIO.effect(g.tx.commit)
      // Make this managed
      _ <- ZIO.effect(g.close)
    } yield ()
  }

  def getParallelism(numberOfEdgesToLoad: Int): EdgeStats = {
    val numPartitions = if (numberOfEdgesToLoad < config.edgeLoader.taskSize) {
      config.edgeLoader.parallelism
    } else {
      scala.math.max((numberOfEdgesToLoad / config.edgeLoader.taskSize) + 1, config.edgeLoader.parallelism)
    }
    EdgeStats(count = numberOfEdgesToLoad, numPartitions)
  }

  override def loadEdges(edges: Dataset[MakeEdge], label: String): ZIO[Logging, Throwable, Unit] = {

    val c       = config
    val jobFunc = job _

    // Add edges to all rows within a partition
    def loadFunc: (Iterator[MakeEdge]) => Unit = (partition: Iterator[MakeEdge]) => {
      val executorJob = withGraph(c, graph => jobFunc(c, graph, label, partition))
      zio.Runtime.default.unsafeRun(executorJob)
    }

    val load = loadFunc

    for {
      numberOfEdgesToLoad <- ZIO.effect(edges.count.toInt)
      stats = getParallelism(numberOfEdgesToLoad)
      // We do .rdd.keyBy here because running edges.foreachPartition strangely results in a very slow deserialization job with small number of partitions
      // Before running the actual load, if there is a repartition just before the call to .rdd
      _ <- ZIO
        .effect(
          edges.sparkSession.sparkContext
            .runJob(edges.rdd.keyBy(_.src).partitionBy(new HashPartitioner(stats.partitions)).values, load)
        )
        .fold(
          f => log.error(s"Error while loading edges $f"),
          _ => log.info(s"Successfully loaded ${stats.count} edges to graph backed by ${config.storage.tableName}")
        )
    } yield ()
  }
}
