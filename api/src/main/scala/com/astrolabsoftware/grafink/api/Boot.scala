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
package com.astrolabsoftware.grafink.api

import cats.effect._
import fs2.Stream.Compiler._
import org.http4s.HttpApp
import org.http4s.implicits._
import org.http4s.server.Router
import org.http4s.server.blaze.BlazeServerBuilder
import zio._
import zio.blocking.Blocking
import zio.clock.Clock
import zio.interop.catz._
import zio.logging.Logging

import com.astrolabsoftware.grafink.api.apiconfig.{ APIConfig, ApiConfig }
import com.astrolabsoftware.grafink.api.service.{ JanusGraphConnectionManager, MgmtService }
import com.astrolabsoftware.grafink.api.service.JanusGraphConnectionManager.JanusGraphConnManagerService
import com.astrolabsoftware.grafink.logging.Logger
import com.astrolabsoftware.grafink.models.GrafinkException
import com.astrolabsoftware.grafink.models.GrafinkException.BadArgumentsException

object Boot extends App {

  type AppEnv     = ApiConfig with JanusGraphConnManagerService with Blocking with Logging
  type ApiTask[A] = RIO[AppEnv with zio.clock.Clock, A]

  /**
   * Parses the command line options using CLParser
   * @param args
   * @return Some of ArgsConfig or None if parsing fails due to invalid arguments
   */
  def parseArgs(args: List[String]): Option[ArgsConfig] = {
    val clParser = new CLParser {}
    val parser   = clParser.parseOptions
    parser.parse(
      args,
      ArgsConfig(confFile = "application.conf")
    )
  }

  override def run(args: List[String]): ZIO[ZEnv, Nothing, zio.ExitCode] = {
    val program = parseArgs(args) match {
      case Some(argsConfig) =>
        val server = for {
          config <- ZIO.access[ApiConfig](_.get)
          _      <- logging.log.info(s"Running with $config")
          httpApp = Router[ApiTask](
            "/" -> MgmtService(config.janusgraph).routes
          ).orNotFound

          exitCode <- runHttp(httpApp, config.app.port)

        } yield exitCode

        val layers =
          Blocking.any ++ Logger.live ++ APIConfig.live(argsConfig.confFile) ++ ((Logger.live) >>> JanusGraphConnectionManager
            .live(100))
        server.provideSomeLayer[ZEnv](layers).orDie
      case None =>
        ZIO.fail(BadArgumentsException("Invalid command line arguments"))
    }

    program.foldM(
      {
        case f: GrafinkException => zio.console.putStrLn(s"Failed ${f.error}").as(GrafinkException.getExitCode(f))
        case fail                => zio.console.putStrLn(s"Failed $fail").as(zio.ExitCode.failure)
      },
      _ => zio.console.putStrLn(s"Shutting Down server").as(zio.ExitCode.success)
    )
  }

  def runHttp[R <: Clock](httpApp: HttpApp[RIO[R, *]], port: Int): ZIO[R, Throwable, zio.ExitCode] = {

    type Task[A] = RIO[R, A]
    ZIO.runtime[R].flatMap { implicit rts =>
      BlazeServerBuilder
        .apply[Task](scala.concurrent.ExecutionContext.Implicits.global)
        .bindHttp(port, "0.0.0.0")
        .withHttpApp(httpApp)
        .serve
        .compile[Task, Task, cats.effect.ExitCode]
        .drain
        .fold(_ => zio.ExitCode.failure, _ => zio.ExitCode.success)
    }
  }
}
