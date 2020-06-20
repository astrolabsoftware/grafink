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

import org.apache.spark.sql.{ DataFrame, Row, SparkSession }
import org.apache.spark.sql.types.DataType
import org.apache.tinkerpop.gremlin.structure.T
import org.janusgraph.graphdb.database.StandardJanusGraph
import zio.{ Has, URLayer, ZIO, ZLayer }
import zio.blocking.Blocking
import zio.logging.Logging

import com.astrolabsoftware.grafink.JanusGraphEnv
import com.astrolabsoftware.grafink.JanusGraphEnv.JanusGraphEnv
import com.astrolabsoftware.grafink.Job.{ JobTime, SparkEnv }
import com.astrolabsoftware.grafink.common.{ PartitionManager, Utils }
import com.astrolabsoftware.grafink.models.JanusGraphConfig
import com.astrolabsoftware.grafink.models.config.Config

object VertexProcessor {

  val vertexDir = "vertex"

  type VertexProcessorService = Has[VertexProcessor.Service]

  trait Service {
    def process(jobTime: JobTime, df: DataFrame): ZIO[Logging, Throwable, Unit]
    def loadVertices(df: DataFrame): ZIO[Logging, Throwable, Unit]
  }

  val live: URLayer[SparkEnv with Has[JanusGraphConfig] with Logging, VertexProcessorService] =
    ZLayer.fromEffect(
      for {
        spark            <- ZIO.access[SparkEnv](_.get.sparkEnv)
        janusGraphConfig <- Config.janusGraphConfig
      } yield new VertexProcessorLive(spark, janusGraphConfig)
    )

  def process(
    jobTime: JobTime,
    df: DataFrame
  ): ZIO[VertexProcessorService with Logging, Throwable, Unit] =
    ZIO.accessM(_.get.process(jobTime, df))

  def load(df: DataFrame): ZIO[VertexProcessorService with Logging, Throwable, Unit] =
    ZIO.accessM(_.get.loadVertices(df))

}

final class VertexProcessorLive(spark: SparkSession, config: JanusGraphConfig) extends VertexProcessor.Service {

  override def process(jobTime: JobTime, dfWithId: DataFrame): ZIO[Logging, Throwable, Unit] = loadVertices(dfWithId)

  override def loadVertices(df: DataFrame): ZIO[Logging, Throwable, Unit] = {

    val c = config
    val dataTypeForVertexPropertyCols: Map[String, DataType] =
      df.schema.fields.map(f => f.name -> f.dataType).toMap

    def loaderFunc: (Iterator[Row]) => Unit = (partition: Iterator[Row]) => {

      val batchSize        = c.vertexLoader.batchSize
      val vertexProperties = c.schema.vertexPropertyCols

      @inline
      def getVertexProperties(r: Row): Seq[AnyRef] =
        vertexProperties.flatMap { property =>
          // Check if the data exists for the property in this alert/row
          if (r.isNullAt(r.fieldIndex(property))) {
            List()
          } else {
            val dType = Utils.getClassTag(dataTypeForVertexPropertyCols(property))
            List(property, r.getAs[dType.type](property))
          }
        }

      val janusGraphLayer = (Blocking.live ++ ZLayer.succeed(c)) >>> JanusGraphEnv.hbase()
      @inline
      def getVertexParams(r: Row, id: java.lang.Long): Seq[AnyRef] = Seq(T.id, id) ++ getVertexProperties(r)

      val executorJob =
        for {
          graph <- ZIO.access[JanusGraphEnv](_.get.graph)
          idManager = graph.asInstanceOf[StandardJanusGraph].getIDManager
          kgroup    = partition.grouped(batchSize)
          l = kgroup.map(group =>
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
          _ <- ZIO.collectAll_(l.toIterable)
          // Additional commit if anything left
          _ <- ZIO.effect(graph.tx.commit)
          /* k = (partition.grouped(batchSize).map { group =>
            group.foreach(r => graph.addVertex(getVertexParams(r, idManager.toVertexId): _*))
            ZIO.effect(graph.tx.commit)
          }) */
          // _ <- ZIO.effect(graph.tx.commit)
        } yield ()
      zio.Runtime.default.unsafeRun(executorJob.provideLayer(janusGraphLayer))
    }

    val load = loaderFunc

    ZIO.effect(spark.sparkContext.runJob(df.rdd, load))
  }
}
