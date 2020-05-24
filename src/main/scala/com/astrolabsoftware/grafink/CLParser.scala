package com.astrolabsoftware.grafink

import buildinfo.BuildInfo
import scopt.OptionParser

case class ArgsConfig(confFile: String)

trait CLParser {

  def parseOptions(): OptionParser[ArgsConfig] =
    new scopt.OptionParser[ArgsConfig]("janusloader") {
      head("janusloader", BuildInfo.version)

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
