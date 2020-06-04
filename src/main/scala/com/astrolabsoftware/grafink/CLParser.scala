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
import java.time.format.DateTimeFormatter

import scala.util.Try

import buildinfo.BuildInfo
import scopt.OptionParser

final case class ArgsConfig(confFile: String, startDate: LocalDate, duration: Int)

/**
 * Command line parser for Grafink
 */
trait CLParser {

  import CLParser._

  /**
   *  This will parse the command line options
   * @return An OptionParser structure that contains the successfully parsed ArgsConfig or default
   */
  def parseOptions(): OptionParser[ArgsConfig] =
    new scopt.OptionParser[ArgsConfig](BuildInfo.name) {
      head(BuildInfo.name, BuildInfo.version)

      opt[String]('c', "config")
        .action((x, c) => c.copy(confFile = x))
        .optional()
        .validate(x =>
          if (new java.io.File(x).exists) success
          else failure("Option --config must be a valid file path")
        )
        .text("config accepts path to a configuration file")

      opt[String]('s', "startdate")
        .required()
        .action((x, c) => c.copy(startDate = LocalDate.parse(x, dateFormat)))
        .validate(x =>
          if (validateDate(x)) success
          else failure("Option --startdate must be a valid date in format (yyyy-MM-dd)")
        )
        .text("startdate accepts start day's date for which the job needs to be run (yyyy-MM-dd)")

      opt[Int]('d', "duration")
        .optional()
        .action((x, c) => c.copy(duration = x))
        .validate(x =>
          if (x > 0 && x <= 7) success
          else failure("Option --duration must be a valid value between 1 and 7 (included)")
        )
        .text(
          "duration accepts duration (# of days) for which the job needs to process the data starting from startdate"
        )
    }

  def validateDate(date: String): Boolean = Try { getLocalDate(date); true }.getOrElse(false)
}

object CLParser {

  val dateFormat: DateTimeFormatter         = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  def getLocalDate(date: String): LocalDate = LocalDate.parse(date, dateFormat)
}
