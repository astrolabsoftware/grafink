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
package com.astrolabsoftware.grafink.processor.vertex

import scala.collection.JavaConverters._

import org.apache.spark.sql.{ DataFrame, Row }
import org.apache.spark.sql.types.DataType
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.apache.tinkerpop.gremlin.structure.T
import org.janusgraph.core.JanusGraph
import org.janusgraph.graphdb.database.StandardJanusGraph
import zio._
import zio.logging.{ log, Logging }

import com.astrolabsoftware.grafink.JanusGraphEnv.withGraph
import com.astrolabsoftware.grafink.common.Utils
import com.astrolabsoftware.grafink.logging.Logger
import com.astrolabsoftware.grafink.models._
import com.astrolabsoftware.grafink.models.config.Config
import com.astrolabsoftware.grafink.processor.vertex.FixedVertexDataReader.FixedVertexDataReader

object VertexProcessor {

  val vertexDir = "vertex"

  type VertexProcessorService = Has[VertexProcessor.Service]

  trait Service {
    def loadFixedVertices(
      graph: JanusGraph,
      fixedVertices: List[FixedVertex]
    ): ZIO[FixedVertexDataReader with Logging, Throwable, Unit]
    def process(df: DataFrame): ZIO[Logging, Throwable, Unit]
    def delete(df: DataFrame): ZIO[Logging, Throwable, Unit]
  }

  val live: URLayer[Has[GrafinkJobConfig] with Has[JanusGraphConfig] with Logging, VertexProcessorService] =
    ZLayer.fromEffect(
      for {
        config <- Config.grafinkJanusGraphConfig
      } yield VertexProcessorLive(config)
    )

  def loadFixedVertices(
    graph: JanusGraph,
    fixedVertices: List[FixedVertex]
  ): ZIO[VertexProcessorService with FixedVertexDataReader with Logging, Throwable, Unit] =
    ZIO.accessM(_.get.loadFixedVertices(graph, fixedVertices))

  def process(df: DataFrame): ZIO[VertexProcessorService with Logging, Throwable, Unit] =
    ZIO.accessM(_.get.process(df))

  def delete(df: DataFrame): ZIO[VertexProcessorService with Logging, Throwable, Unit] =
    ZIO.accessM(_.get.delete(df))
}

final case class VertexProcessorLive(config: GrafinkJanusGraphConfig) extends VertexProcessor.Service {

  // WARNING: Here we have assumed schema vertexLabels will have the configured vertex label in vertex loader
  val labelConfig = config.job.schema.vertexLabels.find(p => p.name == config.job.vertexLoader.label).get

  def job(
    config: GrafinkJobConfig,
    graph: JanusGraph,
    schema: Map[String, DataType],
    partition: Iterator[Row]
  ): ZIO[Any, Throwable, Unit] = {

    val batchSize = config.vertexLoader.batchSize
    val label     = labelConfig
    // Not using fixed vertex properties
    val vertexProperties = label.propertiesFromData

    @inline
    def getVertexProperties(r: Row): Seq[AnyRef] =
      vertexProperties.flatMap { property =>
        // Check if the data exists for the property in this alert/row
        if (r.isNullAt(r.fieldIndex(property))) {
          List()
        } else {
          val dType = Utils.getClassTag(schema(property))
          List(property, r.getAs[dType.type](property))
        }
      }

    @inline
    def getVertexParams(r: Row, id: java.lang.Long): Seq[AnyRef] =
      Seq(T.id, id, T.label, label.name) ++ getVertexProperties(r)

    val idManager = graph.asInstanceOf[StandardJanusGraph].getIDManager
    val l = partition.map(r =>
      for {
        _ <- ZIO.effect(graph.addVertex(
          getVertexParams(r, java.lang.Long.valueOf(idManager.toVertexId(r.getAs[Long]("id")))): _ *
        ))
        _ <- ZIO.effect(graph.tx.commit)
      } yield ()
    )

    for {
      _ <- ZIO.collectAll_(l.toIterable)
      // Additional commit if anything left
      _ <- ZIO.effect(graph.tx.commit)
    } yield ()
  }

  def deleteJob(
    graph: JanusGraph,
    partition: Iterator[Row]
  ): ZIO[Logging, Throwable, Unit] = {

    val g: ZManaged[Any, Nothing, GraphTraversalSource] =
      ZManaged.make(ZIO.succeed(graph.traversal()))(g =>
        ZIO
          .effect(g.close())
          .fold(
            f => log.info(s"Exception closing graph traversal $f"),
            _ => log.info("Graph traversal closed successfully")
          )
      )
    val idManager = graph.asInstanceOf[StandardJanusGraph].getIDManager
    val l = (g: GraphTraversalSource) =>
      partition.map { r =>
        val id = idManager.toVertexId(r.getAs[Long]("id"))
        for {
          vertex <- ZIO.effect(g.V(java.lang.Long.valueOf(id)).next())
          _      <- ZIO.effect(vertex.remove())
          // Strangely need to commit on every removal, otherwise we end up with some vertex not being deleted
          _ <- ZIO.effect(graph.tx().commit()) catchAll (e => log.error(s"Error deleting vertex with id $id : $e"))
        } yield ()
      }

    val effect = (g: GraphTraversalSource) =>
      for {
        _ <- ZIO.collectAll_(l(g).toIterable)
        _ <- ZIO.effect(graph.tx().commit())
      } yield ()
    g.use(g => effect(g))
  }

  def getDataTypeForVertexProperties(vertexProperties: List[String], df: DataFrame): Map[String, DataType] = {
    val vertexPropertiesSet = vertexProperties.toSet
    df.schema.fields.filter(f => vertexPropertiesSet.contains(f.name)).map(f => f.name -> f.dataType).toMap
  }

  override def loadFixedVertices(
    graph: JanusGraph,
    fixedVertices: List[FixedVertex]
  ): ZIO[FixedVertexDataReader with Logging, Throwable, Unit] = {

    val idManager = graph.asInstanceOf[StandardJanusGraph].getIDManager
    val firstId   = java.lang.Long.valueOf(idManager.toVertexId(fixedVertices.head.id))
    for {
      // First check if any of these vertices already exists, if yes, skip loading
      _ <- ZIO
        .effect(graph.traversal())
        .bracket(
          g => ZIO.effect(g.close()).fold(_ => ZIO.unit, _ => ZIO.unit),
          g =>
            for {
              existV <- ZIO.effect(g.V(firstId).toList.asScala)
              _ <- if (existV.size > 0) {
                log.warn(s"Skipping adding fixed vertices as vertex ${existV.head.id} already exists")
              } else {
                ZIO.collectAll_(
                  fixedVertices.map(v =>
                    ZIO.effect(
                      graph.addVertex(
                        (Seq(
                          T.id,
                          java.lang.Long.valueOf(idManager.toVertexId(v.id)),
                          T.label,
                          v.label
                        ) ++ v.properties.flatMap(p => List(p.name, p.value)): Seq[AnyRef]): _*
                      )
                    )
                  )
                )
              }
              _ <- ZIO.effect(graph.tx.commit)
            } yield ()
        )
    } yield ()
  }

  override def process(df: DataFrame): ZIO[Logging, Throwable, Unit] = {

    val jobConfig                                            = config.job
    val c                                                    = config
    val vertexProperties                                     = labelConfig.propertiesFromData
    val dataTypeForVertexPropertyCols: Map[String, DataType] = getDataTypeForVertexProperties(vertexProperties, df)
    val jobFunc                                              = job _

    def loadFunc: (Iterator[Row]) => Unit = (partition: Iterator[Row]) => {
      val executorJob = withGraph(c, graph => jobFunc(jobConfig, graph, dataTypeForVertexPropertyCols, partition))
      zio.Runtime.default.unsafeRun(executorJob)
    }

    val load = loadFunc

    ZIO.effect(df.foreachPartition(load))
  }

  override def delete(df: DataFrame): ZIO[Logging, Throwable, Unit] = {
    val jobFunc = deleteJob _
    def deleteFunc: (Iterator[Row]) => Unit = (partition: Iterator[Row]) => {
      val c           = config
      val executorJob = withGraph(c, graph => jobFunc(graph, partition))
      zio.Runtime.default.unsafeRun(executorJob.provideLayer(Logger.live))
    }
    val delete = deleteFunc
    ZIO.effect(df.foreachPartition(delete))
  }
}
