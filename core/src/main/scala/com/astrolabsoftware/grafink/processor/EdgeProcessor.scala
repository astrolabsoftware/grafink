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

import com.github.mrpowers.spark.daria.sql.DataFrameHelpers._
import org.apache.spark.HashPartitioner
import org.apache.spark.sql.{ DataFrame, Row }
import org.janusgraph.core.JanusGraph
import org.janusgraph.graphdb.database.StandardJanusGraph
import zio._
import zio.logging.{ log, Logging }

import com.astrolabsoftware.grafink.JanusGraphEnv.withGraph
import com.astrolabsoftware.grafink.Job.VertexData
import com.astrolabsoftware.grafink.models.{ GrafinkJanusGraphConfig, GrafinkJobConfig, JanusGraphConfig }
import com.astrolabsoftware.grafink.models.config.Config
import com.astrolabsoftware.grafink.processor.EdgeProcessor.{ EdgeColumns, EdgeStats }
import com.astrolabsoftware.grafink.processor.edgerules.VertexClassifierRule

/**
 * Interface that loads the edges data into JanusGraph
 */
object EdgeProcessor {

  case class EdgeStats(count: Int, partitions: Int)

  object EdgeColumns {
    val SRCVERTEXFIELD   = "src"
    val DSTVERTEXFIELD   = "dst"
    val PROPERTYVALFIELD = "propVal"
  }
  val requiredEdgeColumns = Seq(EdgeColumns.SRCVERTEXFIELD, EdgeColumns.DSTVERTEXFIELD, EdgeColumns.PROPERTYVALFIELD)

  type EdgeProcessorService = Has[EdgeProcessor.Service]

  trait Service {
    def process(vertexData: VertexData, rules: List[VertexClassifierRule]): ZIO[Logging, Throwable, Unit]
    def loadEdges(edgesRDD: DataFrame, label: String, edgePropKey: String): ZIO[Logging, Throwable, Unit]
  }

  /**
   * Get the EdgeProcessLive instance
   */
  val live: URLayer[Has[GrafinkJobConfig] with Has[JanusGraphConfig] with Logging, EdgeProcessorService] =
    ZLayer.fromEffect(
      for {
        config <- Config.grafinkJanusGraphConfig
      } yield EdgeProcessorLive(config)
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
final case class EdgeProcessorLive(config: GrafinkJanusGraphConfig) extends EdgeProcessor.Service {

  override def process(vertexData: VertexData, rules: List[VertexClassifierRule]): ZIO[Logging, Throwable, Unit] =
    for {
      _ <- ZIO.collectAll(rules.map { rule =>
        for {
          _ <- log.info(s"Adding edges using rule ${rule.name}")
          edges = rule.classify(vertexData.loaded, vertexData.current)
          // Make sure the edges from the rule have the required mandatory columns
          // This is unfortunate result of lack of variant types in Datasets and also
          // lack of custom encoder creation capability for spark Datasets
          // https://issues.apache.org/jira/browse/SPARK-22351
          _ <- ZIO.effect(validatePresenceOfColumns(edges, EdgeProcessor.requiredEdgeColumns))
          _ <- loadEdges(edges, rule.getEdgeLabel, rule.getEdgePropertyKey)
        } yield ()
      })
    } yield ()

  def job(
    config: GrafinkJobConfig,
    graph: JanusGraph,
    label: String,
    propertyKey: String,
    partition: Iterator[Row]
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
              val propertyVal = r.getAs[AnyVal](EdgeColumns.PROPERTYVALFIELD)

              // TODO: Optimize
              for {
                srcVertex <- ZIO.effect(
                  g.V(java.lang.Long.valueOf(idManager.toVertexId(r.getAs[Long](EdgeColumns.SRCVERTEXFIELD))))
                )
                dstVertex <- ZIO.effect(
                  g.V(java.lang.Long.valueOf(idManager.toVertexId(r.getAs[Long](EdgeColumns.DSTVERTEXFIELD))))
                )
                _ <- ZIO.effect(srcVertex.addE(label).to(dstVertex).property(propertyKey, propertyVal).iterate)
                // Add reverse edge as well
                srcVertex <- ZIO.effect(
                  g.V(java.lang.Long.valueOf(idManager.toVertexId(r.getAs[Long](EdgeColumns.SRCVERTEXFIELD))))
                )
                dstVertex <- ZIO.effect(
                  g.V(java.lang.Long.valueOf(idManager.toVertexId(r.getAs[Long](EdgeColumns.DSTVERTEXFIELD))))
                )
                _ <- ZIO.effect(dstVertex.addE(label).to(srcVertex).property(propertyKey, propertyVal).iterate)
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
    val jobConfig = config.job
    val numPartitions = if (numberOfEdgesToLoad < jobConfig.edgeLoader.taskSize) {
      jobConfig.edgeLoader.parallelism
    } else {
      scala.math.max((numberOfEdgesToLoad / jobConfig.edgeLoader.taskSize) + 1, jobConfig.edgeLoader.parallelism)
    }
    EdgeStats(count = numberOfEdgesToLoad, numPartitions)
  }

  override def loadEdges(edges: DataFrame, label: String, propertyKey: String): ZIO[Logging, Throwable, Unit] = {

    val jobConfig = config.job
    val c         = config
    val jobFunc   = job _

    // Add edges to all rows within a partition
    def loadFunc: (Iterator[Row]) => Unit = (partition: Iterator[Row]) => {
      val executorJob = withGraph(c, graph => jobFunc(jobConfig, graph, label, propertyKey, partition))
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
            .runJob(
              edges.rdd
                .keyBy(r => r.getAs[Long](EdgeColumns.SRCVERTEXFIELD))
                .partitionBy(new HashPartitioner(stats.partitions))
                .values,
              load
            )
        )
        .fold(
          f => log.error(s"Error while loading edges $f"),
          _ =>
            log.info(s"Successfully loaded ${stats.count} edges to graph backed by ${c.janusGraph.storage.tableName}")
        )
    } yield ()
  }
}
