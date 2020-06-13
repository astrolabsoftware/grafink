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

import org.apache.spark.sql.{ DataFrame, Row, SaveMode, SparkExtensions, SparkSession }
import org.apache.tinkerpop.gremlin.structure.T
import org.janusgraph.core.JanusGraph
import org.janusgraph.graphdb.database.StandardJanusGraph
import zio.{ Has, URLayer, ZIO, ZLayer }
import zio.logging.Logging

import com.astrolabsoftware.grafink.JanusGraphEnv.JanusGraphEnv
import com.astrolabsoftware.grafink.Job.{ JobTime, SparkEnv }
import com.astrolabsoftware.grafink.common.PartitionManager
import com.astrolabsoftware.grafink.models.JanusGraphConfig
import com.astrolabsoftware.grafink.models.config.Config
import com.astrolabsoftware.grafink.services.IDManager.{ IDManagerService, IDType }

object VertexProcessor {

  val vertexDir = "vertex"

  type VertexProcessorService = Has[VertexProcessor.Service]

  trait Service {
    def process(jobTime: JobTime, df: DataFrame): ZIO[IDManagerService with Logging, Throwable, Unit]
    def load(df: DataFrame): ZIO[Logging, Throwable, Unit]
  }

  val live: URLayer[SparkEnv with Has[JanusGraphConfig] with JanusGraphEnv with Logging, VertexProcessorService] =
    ZLayer.fromEffect(
      for {
        spark            <- ZIO.access[SparkEnv](_.get.sparkEnv)
        graph            <- ZIO.access[JanusGraphEnv](_.get.graph)
        janusGraphConfig <- Config.janusGraphConfig
      } yield new VertexProcessorLive(spark, graph, janusGraphConfig)
    )

  def process(
    jobTime: JobTime,
    df: DataFrame
  ): ZIO[VertexProcessorService with IDManagerService with Logging, Throwable, Unit] =
    ZIO.accessM(_.get.process(jobTime, df))

  def load(df: DataFrame): ZIO[VertexProcessorService with Logging, Throwable, Unit] =
    ZIO.accessM(_.get.load(df))

}

final class VertexProcessorLive(spark: SparkSession, graph: JanusGraph, config: JanusGraphConfig)
    extends VertexProcessor.Service {

  def addId(df: DataFrame, lastMax: IDType): DataFrame =
    SparkExtensions.zipWithIndex(df, lastMax + 1)

  override def process(jobTime: JobTime, df: DataFrame): ZIO[IDManagerService with Logging, Throwable, Unit] = {

    val mode = SaveMode.Append

    for {
      idManager <- ZIO.access[IDManagerService](_.get)
      lastMax   <- idManager.fetchID(jobTime)
      dfWithId = addId(df, lastMax)
      // Write intermediate data
      // TODO: Repartition to generate desired number of output files
      _ <- ZIO.effect(
        dfWithId.write
          .format("parquet")
          .mode(mode)
          .partitionBy(PartitionManager.partitionColumns: _*)
          .save(idManager.config.spark.dataPath)
      )
    } yield {
      // Finally load to Janusgraph
      load(dfWithId)
    }
  }

  override def load(df: DataFrame): ZIO[Logging, Throwable, Unit] = {

    val getId: Row => Long = r => r.getAs[Long]("id")
    val serializableGraph  = graph
    val batchSize          = config.vertexLoader.batchSize

    def loaderFunc: (Iterator[Row]) => Unit = (partition: Iterator[Row]) => {

      // Ugly but unfortunate result of API structure
      val idManager = graph.asInstanceOf[StandardJanusGraph].getIDManager

      partition.grouped(batchSize).foreach { group =>
        val params: Row => Seq[AnyRef] = r => Seq(T.id, java.lang.Long.valueOf(idManager.toVertexId(getId(r))))
        group.foreach(r => serializableGraph.addVertex(params(r)))
        serializableGraph.tx.commit
      }
      serializableGraph.tx.commit
    }

    val load = loaderFunc

    ZIO.effect(spark.sparkContext.runJob(df.rdd, load))
  }
}
