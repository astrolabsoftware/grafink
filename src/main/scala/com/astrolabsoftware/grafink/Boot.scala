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

import buildinfo.BuildInfo
import cats.implicits._
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import pureconfig._
import pureconfig.generic.ProductHint
import pureconfig.generic.auto._

import com.astrolabsoftware.grafink.models.ReaderConfig

object Boot {

  // For pure config to be able to use camel case
  implicit def hint[T]: ProductHint[T] = ProductHint[T](ConfigFieldMapping(CamelCase, CamelCase))

  def main(args: Array[String]): Unit = {
    val clParser = new CLParser {}
    val parser = clParser.parseOptions

    parser.parse(
      args,
      ArgsConfig(confFile = "application.conf")
    ) match {
      case Some(config) =>
        implicit val spark = SparkSession
          .builder()
          .appName(BuildInfo.name)
          .getOrCreate()
        try {

          // Get conf
          val conf = ConfigFactory.parseFile(new File(config.confFile))

          for {
            readerConf <- ConfigSource.fromConfig(conf).at("reader").load[ReaderConfig]

          } yield {

          }

        } finally {
          spark.stop()
        }
      case None =>
    }
  }
}
