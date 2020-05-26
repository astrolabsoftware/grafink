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

import buildinfo.BuildInfo
import scopt.OptionParser

case class ArgsConfig(confFile: String)

/**
 * Command line parser for grafink
 */
trait CLParser {

  /**
   *  This will parse the command line options
   * @return An OptionParser structure that contains the successfully parsed ArgsConfig or default
   */
  def parseOptions(): OptionParser[ArgsConfig] =
    new scopt.OptionParser[ArgsConfig]("janusloader") {
      head("grafink", BuildInfo.version)

      opt[String]('c', "config")
        .action((x, c) => c.copy(confFile = x))
        .optional()
        .validate(x =>
          if (new java.io.File(x).exists) success
          else failure("Option --config must be a valid file path")
        )
        .text("config accepts path to a configuration file")
    }
}

object CLParser {}
