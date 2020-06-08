package com.astrolabsoftware.grafink.processor

import org.apache.spark.sql.{ DataFrame, Row, SaveMode, SparkSession }
import org.apache.spark.sql.expressions.{ UserDefinedFunction, Window }
import org.apache.spark.sql.functions.{ col, row_number, spark_partition_id, udf }
import org.apache.tinkerpop.gremlin.structure.T
import org.janusgraph.core.JanusGraphFactory
import org.janusgraph.diskstorage.util.time.TimestampProviders
import org.janusgraph.graphdb.database.StandardJanusGraph
import zio.{ Has, URLayer, ZIO, ZLayer }
import zio.logging.Logging

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

  val live: URLayer[SparkEnv with Logging with Has[JanusGraphConfig], VertexProcessorService] =
    ZLayer.fromEffect(
      for {
        spark            <- ZIO.access[SparkEnv](_.get.sparkEnv)
        janusGraphConfig <- Config.janusGraphConfig
      } yield new VertexProcessorLive(spark, janusGraphConfig)
    )

  def process(df: DataFrame, outputBasePath: String): ZIO[VertexProcessorService with Logging, Throwable, Unit] =
    ZIO.accessM(_.get.process(df, outputBasePath))

  def load(df: DataFrame): ZIO[VertexProcessorService with Logging, Throwable, Unit] =
    ZIO.accessM(_.get.load(df))

}

final class VertexProcessorLive(spark: SparkSession, config: JanusGraphConfig) extends VertexProcessor.Service {

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

    def loaderFunc: (Iterator[Row]) => Unit = (partition: Iterator[Row]) => {
      for {
        graph <- ZIO.effect(
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
            // Use batch loading
            .set("storage.batch-loading", true)
            // Allow setting vertex ids
            .set("graph.set-vertex-id", true)
            .open()
        )
        // Ugly but unfortunate result of API structure
        idManager = graph.asInstanceOf[StandardJanusGraph].getIDManager

      } yield {
        val batchSize = config.vertexLoader.batchSize
        partition.grouped(batchSize).foreach { group =>
          val params: Row => Seq[AnyRef] = r => Seq(T.id, java.lang.Long.valueOf(idManager.toVertexId(getId(r))))
          group.foreach(r => graph.addVertex(params(r)))
          graph.tx.commit
        }
        graph.tx.commit
      }
    }

    val load = loaderFunc

    ZIO.effect(spark.sparkContext.runJob(df.rdd, load))

  }
}
