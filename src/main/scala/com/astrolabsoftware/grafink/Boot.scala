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

import java.io.File
import java.time.LocalDate
import java.time.temporal.ChronoUnit

import zio._
import zio.blocking.Blocking
import zio.console.Console

import com.astrolabsoftware.grafink.Job.JobTime
import com.astrolabsoftware.grafink.logging.Logger
import com.astrolabsoftware.grafink.models.GrafinkException
import com.astrolabsoftware.grafink.models.GrafinkException.BadArgumentsException
import com.astrolabsoftware.grafink.models.config.Config
import com.astrolabsoftware.grafink.processor.{ EdgeProcessor, VertexProcessor }
import com.astrolabsoftware.grafink.schema.SchemaLoader
import com.astrolabsoftware.grafink.services.{ IDManager, IDManagerSparkService }
import com.astrolabsoftware.grafink.services.reader.Reader

/**
 * Contains the entry point to the spark job
 */
object Boot extends App {

  /**
   * Default startdate value
   */
  val yesterdayDate = LocalDate.now.minus(1, ChronoUnit.DAYS)

  /**
   * Parses the command line options using CLParser
   * @param args
   * @return Some of ArgsConfig or None if parsing fails due to invalid arguments
   */
  def parseArgs(args: Array[String]): Option[ArgsConfig] = {
    val clParser = new CLParser {}
    val parser   = clParser.parseOptions
    parser.parse(
      args,
      ArgsConfig(confFile = "application.conf", startDate = yesterdayDate, duration = 1)
    )
  }

  override def run(args: List[String]): ZIO[ZEnv, Nothing, ExitCode] = {

    val program = parseArgs(args.toArray) match {
      case Some(argsConfig) =>
        val logger      = Logger.live
        val configLayer = logger >>> Config.live(argsConfig.confFile)
        val sparkLayer  = ZLayer.requires[Blocking] >>> SparkEnv.cluster
        // val janusGraphLayer      = ZLayer.requires[Blocking] ++ configLayer >>> JanusGraphEnv.hbase
        val schemaLoaderLayer    = logger ++ configLayer >>> SchemaLoader.live
        val idManagerLayer       = logger ++ sparkLayer ++ configLayer >>> IDManagerSparkService.live
        val readerLayer          = logger ++ sparkLayer ++ configLayer >>> Reader.live
        val vertexProcessorLayer = logger ++ sparkLayer ++ configLayer >>> VertexProcessor.live
        val edgeProcessorLayer   = logger ++ sparkLayer ++ configLayer >>> EdgeProcessor.live

        Job
          .runGrafinkJob(JobTime(argsConfig.startDate, argsConfig.duration))
          .provideCustomLayer(
            SparkEnv.cluster ++
              configLayer ++
              schemaLoaderLayer ++
              readerLayer ++
              idManagerLayer ++
              vertexProcessorLayer ++
              edgeProcessorLayer ++
              logger
          )
      case None =>
        ZIO.fail(BadArgumentsException("Invalid command line arguments"))
    }

    program.foldM(
      {
        case f: GrafinkException => console.putStrLn(s"Failed ${f.error}").as(GrafinkException.getExitCode(f))
        case fail                => console.putStrLn(s"Failed $fail").as(ExitCode.failure)
      },
      _ => console.putStrLn(s"Succeeded").as(ExitCode.success)
    )
  }
}
