package com.astrolabsoftware.grafink.common

import java.time.LocalDate

import org.apache.hadoop.fs.FileSystem
import zio.{ console, ZIO }
import zio.blocking.Blocking
import zio.test._
import zio.test.Assertion._
import zio.test.environment.{ TestClock, TestConsole }

import com.astrolabsoftware.grafink.common.PartitionManager.dateFormat
import com.astrolabsoftware.grafink.logging.Logger
import com.astrolabsoftware.grafink.utils.SparkTestEnv

object PartitionManagerSpec extends DefaultRunnableSpec {

  def spec: ZSpec[Environment, Failure] =
    suite("PartitionManagerSpec")(
      test("PartitionManager will correctly generate read paths") {
        val dateString       = "2019-02-01"
        val date             = LocalDate.parse(dateString, dateFormat)
        val partitionManager = PartitionManager(startDate = date, duration = 2)

        assert(partitionManager.partitionPaths)(
          hasSameElements(List(PartitionPath("2019", "02", "01"), PartitionPath("2019", "02", "02")))
        )
      },
      testM("PartitionManager will correctly filter out non-existing paths") {
        val dataPath         = "/data"
        val path             = getClass.getResource(dataPath).getPath
        val dateString       = "2019-02-01"
        val date             = LocalDate.parse(dateString, dateFormat)
        val partitionManager = PartitionManager(startDate = date, duration = 7)

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
      }
    )
}
