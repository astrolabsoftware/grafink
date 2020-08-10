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

import org.apache.spark.sql.{ DataFrame, Row }
import org.apache.spark.sql.types.DataType
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.{ GraphTraversal, GraphTraversalSource }
import org.apache.tinkerpop.gremlin.structure.T
import org.janusgraph.core.JanusGraph
import org.janusgraph.graphdb.database.StandardJanusGraph
import zio.{ Has, URLayer, ZIO, ZLayer, ZManaged }
import zio.logging.{ log, Logging }

import com.astrolabsoftware.grafink.JanusGraphEnv.withGraph
import com.astrolabsoftware.grafink.common.Utils
import com.astrolabsoftware.grafink.logging.Logger
import com.astrolabsoftware.grafink.models.{ GrafinkJanusGraphConfig, GrafinkJobConfig, JanusGraphConfig }
import com.astrolabsoftware.grafink.models.config.Config

object VertexProcessor {

  val vertexDir = "vertex"

  type VertexProcessorService = Has[VertexProcessor.Service]

  trait Service {
    def process(df: DataFrame): ZIO[Logging, Throwable, Unit]
    def delete(df: DataFrame): ZIO[Logging, Throwable, Unit]
  }

  val live: URLayer[Has[GrafinkJobConfig] with Has[JanusGraphConfig] with Logging, VertexProcessorService] =
    ZLayer.fromEffect(
      for {
        config <- Config.grafinkJanusGraphConfig
      } yield VertexProcessorLive(config)
    )

  def process(df: DataFrame): ZIO[VertexProcessorService with Logging, Throwable, Unit] =
    ZIO.accessM(_.get.process(df))

  def delete(df: DataFrame): ZIO[VertexProcessorService with Logging, Throwable, Unit] =
    ZIO.accessM(_.get.delete(df))
}

final case class VertexProcessorLive(config: GrafinkJanusGraphConfig) extends VertexProcessor.Service {

  def job(
    config: GrafinkJobConfig,
    graph: JanusGraph,
    schema: Map[String, DataType],
    partition: Iterator[Row]
  ): ZIO[Any, Throwable, Unit] = {

    val batchSize        = config.vertexLoader.batchSize
    val vertexProperties = config.schema.vertexPropertyCols

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
    def getVertexParams(r: Row, id: java.lang.Long): Seq[AnyRef] = Seq(T.id, id) ++ getVertexProperties(r)

    val idManager = graph.asInstanceOf[StandardJanusGraph].getIDManager
    val kgroup    = partition.grouped(batchSize)
    val l = kgroup.map(group =>
      for {
        _ <- ZIO.collectAll_(
          // TODO: Make id fieldName configurable
          group.map(r =>
            ZIO.effect(
              graph.addVertex(
                getVertexParams(r, java.lang.Long.valueOf(idManager.toVertexId(r.getAs[Long]("id")))): _*
              )
            )
          )
        )
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

  override def process(df: DataFrame): ZIO[Logging, Throwable, Unit] = {

    val jobConfig                                            = config.job
    val c                                                    = config
    val vertexProperties                                     = jobConfig.schema.vertexPropertyCols
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
