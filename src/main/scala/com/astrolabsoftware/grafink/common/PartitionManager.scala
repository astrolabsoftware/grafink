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
package com.astrolabsoftware.grafink.common

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

import scala.annotation.tailrec

import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.spark.sql.SparkSession
import zio.{ RIO, Task, ZIO }
import zio.logging.{ log, Logging }

case class PartitionPath(year: String, month: String, day: String)

case class PartitionManager(startDate: LocalDate, duration: Int) {

  import PartitionManager._

  implicit def toPartitionPath(date: LocalDate): PartitionPath =
    PartitionPath(
      year = s"${f"${date.getYear}%02d"}",
      month = paddedInt(date.getMonth.getValue),
      day = paddedInt(date.getDayOfMonth)
    )

  @tailrec
  private def paths(startDate: LocalDate, duration: Int, p: List[PartitionPath]): List[PartitionPath] = {
    if (duration <= 0) return p
    val nextDate = startDate.plus(1, ChronoUnit.DAYS)
    paths(nextDate, duration - 1, nextDate :: p)
  }

  val partitionPaths: List[PartitionPath] = paths(startDate, duration - 1, startDate :: Nil)

  def testDirExist(fs: FileSystem, path: String): ZIO[Logging, Throwable, Boolean] = {
    val p = new Path(path)
    for {
      result <- ZIO.effect(fs.exists(p) && fs.getFileStatus(p).isDirectory)
      // TODO: Maybe some better way of printing this log?
      _ <- if (result == false) log.warn(s"Filtering out $path as no such directory exists") else ZIO.succeed(Unit)
    } yield result
  }

  def getValidPartitionPathStrings(basePath: String, fs: FileSystem): ZIO[Logging, Throwable, List[String]] = {
    val validPaths       = partitionPaths
    val validPathStrings = validPaths.map(toPathString(basePath, _))
    ZIO.filter(validPathStrings)(testDirExist(fs, _))
  }

  def deletePartitions(basePath: String, fs: FileSystem): ZIO[Logging, Throwable, Unit] =
    for {
      paths <- getValidPartitionPathStrings(basePath, fs)
    } yield ZIO
      .collectAll(paths.map { path =>
        RIO
          .effect(fs.delete(new Path(path), true))
          .fold(
            fail => log.error(s"error deleting path $path, failure: $fail"),
            _ => log.info(s"deleted partition path $path")
          )
      })
      .ignore

}

object PartitionManager {

  val paddedInt: Int => String       = (i: Int) => s"${f"${i}%02d"}"
  val dateFormat: DateTimeFormatter  = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  val partitionColumns: List[String] = List("year", "month", "day")

  def apply(duration: Int): PartitionManager = PartitionManager(LocalDate.now, duration)

  def toPathString(base: String, readPath: PartitionPath): String =
    partitionColumns
      .zip(List(readPath.year, readPath.month, readPath.day))
      .foldLeft(base)((a, b) => s"$a/${b._1}=${b._2}")
}
