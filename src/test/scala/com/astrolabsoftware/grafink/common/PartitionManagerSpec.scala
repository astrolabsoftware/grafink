package com.astrolabsoftware.grafink.common

import java.time.LocalDate

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ IntegerType, StringType, StructField, StructType }
import zio.{ console, ZIO, ZLayer }
import zio.blocking.Blocking
import zio.test._
import zio.test.Assertion._
import zio.test.environment.TestConsole

import com.astrolabsoftware.grafink.common.PartitionManager.dateFormat
import com.astrolabsoftware.grafink.logging.Logger
import com.astrolabsoftware.grafink.models._
import com.astrolabsoftware.grafink.services.IDManagerSparkService
import com.astrolabsoftware.grafink.services.IDManagerSparkService.IDManagerSparkService
import com.astrolabsoftware.grafink.utils.{ SparkTestEnv, TempDirService }

object PartitionManagerSpec extends DefaultRunnableSpec {

  def spec: ZSpec[Environment, Failure] =
    suite("PartitionManagerSpec")(
      test("PartitionManagerImpl will correctly generate read paths") {
        val dateString       = "2019-02-01"
        val date             = LocalDate.parse(dateString, dateFormat)
        val partitionManager = PartitionManagerImpl(startDate = date, duration = 2)

        assert(partitionManager.partitionPaths)(
          hasSameElements(List(PartitionPath("2019", "2", "1"), PartitionPath("2019", "2", "2")))
        )
      },
      test("PaddedPartitionManager will correctly generate read paths") {
        val dateString       = "2019-02-01"
        val date             = LocalDate.parse(dateString, dateFormat)
        val partitionManager = PaddedPartitionManager(startDate = date, duration = 2)

        assert(partitionManager.partitionPaths)(
          hasSameElements(List(PartitionPath("2019", "02", "01"), PartitionPath("2019", "02", "02")))
        )
      },
      testM("PaddedPartitionManager will correctly filter out non-existing paths") {
        val dataPath         = "/data"
        val path             = getClass.getResource(dataPath).getPath
        val dateString       = "2019-02-01"
        val date             = LocalDate.parse(dateString, dateFormat)
        val partitionManager = PaddedPartitionManager(startDate = date, duration = 7)

        val sparkLayer = SparkTestEnv.test

        val app = for {
          spark <- SparkTestEnv.sparkEnv
          fs    <- ZIO.effect(FileSystem.get(spark.sparkContext.hadoopConfiguration))
          paths <- partitionManager
            .getValidPartitionPathStrings(path, fs)
            .ensuring(
              ZIO
                .effect(fs.close())
                .fold(
                  failure => console.putStrLn(s"Error closing filesystem: $failure"),
                  _ => console.putStrLn(s"Filesystem closed")
                )
            )
        } yield paths

        assertM(app.provideLayer((Blocking.live >>> sparkLayer) ++ Logger.test))(
          hasSameElements(List(s"$path/year=2019/month=02/day=01"))
        )
      },
      testM("PartitionManager will correctly delete partitions") {
        case class SampleData(payLoad: String, year: Int, month: Int, day: Int)

        val dateString          = "2019-02-01"
        val date                = LocalDate.parse(dateString, dateFormat)
        val tempDirServiceLayer = ((zio.console.Console.live) >>> TempDirService.test)
        val runtime             = zio.Runtime.default
        val tempDir =
          runtime.unsafeRun(TempDirService.createTempDir.provideLayer(tempDirServiceLayer ++ zio.console.Console.live))

        val janusConfig =
          JanusGraphConfig(
            SchemaConfig(
              vertexPropertyCols = List("rfscore", "snnscore", "objectId"),
              vertexLabel = "type",
              edgeLabels = List(),
              index = IndexConfig(composite = List.empty, mixed = List.empty, edge = List.empty)
            ),
            VertexLoaderConfig(10),
            EdgeLoaderConfig(10, 1, 25000, EdgeRulesConfig(SimilarityConfig(""))),
            JanusGraphStorageConfig("", 0, tableName = "test")
          )

        val idManagerConfig =
          IDManagerConfig(IDManagerSparkConfig(tempDir.getAbsolutePath, false), HBaseColumnConfig("", "", ""))

        val idManagerConfigLayer = ZLayer.succeed(idManagerConfig)

        // We are working with 4 days from date
        val partitionManager = PartitionManagerImpl(date, 4)

        val sampleSchema = StructType(
          fields = Array(
            StructField("payLoad", StringType),
            StructField("year", IntegerType),
            StructField("month", IntegerType),
            StructField("day", IntegerType)
          )
        )
        val app =
          for {
            spark <- SparkTestEnv.sparkEnv
            df = spark.createDataFrame(
              spark.sparkContext.parallelize(
                List(
                  Row("Some", 2019, 2, 1),
                  Row("Some", 2019, 2, 3),
                  Row("Some", 2019, 2, 5)
                )
              ),
              sampleSchema
            )
            idManager <- ZIO.access[IDManagerSparkService](_.get)
            // This will create the partitions and write the data
            vertexData <- idManager.process(df, janusConfig.storage.tableName)
            basePath = s"${idManagerConfig.spark.dataPath}/${janusConfig.storage.tableName}"
            _             <- Utils.withFileSystem(fs => partitionManager.deletePartitions(basePath, fs))(df.sparkSession)
            afterDeletion <- ZIO.effect(spark.read.parquet(basePath).drop("id").collect)
            _             <- TempDirService.removeTempDir(tempDir)
          } yield afterDeletion.toList

        val logger     = Logger.test
        val sparkLayer = SparkTestEnv.test
        val idManagerLayer =
          (logger ++ sparkLayer ++ ((tempDirServiceLayer ++ TestConsole.debug) >>> idManagerConfigLayer)) >>> IDManagerSparkService.live

        val layer =
          tempDirServiceLayer ++ TestConsole.debug ++ idManagerLayer ++ logger ++ sparkLayer
        // Only data for 5th day is retained
        assertM(app.provideLayer(layer))(hasSameElementsDistinct(List(Row("Some", 2019, 2, 5))))
      }
    )
}
