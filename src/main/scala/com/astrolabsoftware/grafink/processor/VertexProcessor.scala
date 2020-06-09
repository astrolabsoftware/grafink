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

import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions.{col, row_number, spark_partition_id, udf}
import org.apache.tinkerpop.gremlin.structure.T
import org.janusgraph.core.{JanusGraph, JanusGraphFactory}
import org.janusgraph.graphdb.database.StandardJanusGraph
import zio.{Has, URLayer, ZIO, ZLayer}
import zio.logging.Logging

import com.astrolabsoftware.grafink.JanusGraphEnv.JanusGraphEnv
import com.astrolabsoftware.grafink.Job.SparkEnv
import com.astrolabsoftware.grafink.models.JanusGraphConfig
import com.astrolabsoftware.grafink.models.config.Config

object VertexProcessor {

  val vertexDir = "vertex"

  type VertexProcessorService = Has[VertexProcessor.Service]

  trait Service {
    def process(df: DataFrame, outputBasePath: String): ZIO[Logging, Throwable, Unit]
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

  def process(df: DataFrame, outputBasePath: String): ZIO[VertexProcessorService with Logging, Throwable, Unit] =
    ZIO.accessM(_.get.process(df, outputBasePath))

  def load(df: DataFrame): ZIO[VertexProcessorService with Logging, Throwable, Unit] =
    ZIO.accessM(_.get.load(df))

}

final class VertexProcessorLive(spark: SparkSession, graph: JanusGraph, config: JanusGraphConfig) extends VertexProcessor.Service {

  def addId(df: DataFrame): ZIO[Logging, Throwable, DataFrame] =
    for {
      partitionWithSize <- ZIO.effect(
        df.rdd.mapPartitionsWithIndex { case (i, rows) => Iterator((i, rows.size)) }.collect.sortBy(_._1).map(_._2)
      )
      cumulativePartitionWithSize = partitionWithSize.tail.scan(partitionWithSize.head)(_ + _)
    } yield {
      // Spark udf to calculate id
      val genId: UserDefinedFunction =
        udf((partitionId: Int, rowNumber: Int) => cumulativePartitionWithSize(partitionId) + rowNumber)

      val window = Window.partitionBy("pid")
      df.withColumn("pid", spark_partition_id())
        .withColumn("row_number", row_number().over(window))
        .withColumn("id", genId(col("pid"), col("row_number")))
        .drop("pid", "row_number")
    }

  override def process(df: DataFrame, outputBasePath: String): ZIO[Logging, Throwable, Unit] = {

    val mode = SaveMode.Append

    for {
      dfWithId <- addId(df)
      // Write intermediate data
      // TODO: Repartition to generate desired number of output files
      _ <- ZIO.effect(
        dfWithId.write
          .format("parquet")
          .mode(mode)
          .partitionBy("year", "month", "day", "hour")
          .save(outputBasePath)
      )
    } yield {
      // Finally load to Janusgraph
      load(dfWithId)
    }
  }

  override def load(df: DataFrame): ZIO[Logging, Throwable, Unit] = {

    val getId: Row => Long = r => r.getAs[Long]("id")
    val serializableGraph = graph
    val batchSize = config.vertexLoader.batchSize

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
