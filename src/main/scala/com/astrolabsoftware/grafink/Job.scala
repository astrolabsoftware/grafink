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
package com.astrolabsoftware.grafink

import java.time.LocalDate

import zio._
import zio.blocking.Blocking
import zio.console.Console

import com.astrolabsoftware.grafink.models.config._

// The core application
object Job {

  case class JobTime(day: LocalDate, duration: Int)

  type SparkEnv = Has[SparkEnv.Service]

  /**
   * Runs the spark job to load data into JanusGraph
   */
  val runGrafinkJob: JobTime => ZIO[SparkEnv with GrafinkConfig with Console with Blocking, Throwable, Unit] =
    jobTime =>
      for {
        spark <- ZIO.access[SparkEnv](_.get.sparkEnv)
        // TODO: Insert the processing here
        result <- ZIO
          .succeed("Success")
          .ensuring(
            ZIO
              .effect(spark.stop())
              .fold(
                failure => zio.console.putStrLn(s"Error stopping SparkSession: $failure"),
                _ => zio.console.putStrLn(s"SparkSession stopped")
              )
          )
        // result  <- zio.blocking.effectBlocking()
        _ <- zio.console.putStrLn(s"Executed something with spark ${spark.version}: $result")
      } yield ()
}
