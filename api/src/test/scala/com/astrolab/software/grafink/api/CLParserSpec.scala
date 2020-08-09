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
package com.astrolab.software.grafink.api

import zio.test._
import zio.test.Assertion._

import com.astrolabsoftware.grafink.api.{ ArgsConfig, CLParser }

object CLParserSpec extends DefaultRunnableSpec {

  val clParser = new CLParser {}
  val cFile    = getClass.getResource("/application.conf").getPath

  def spec: ZSpec[Environment, Failure] =
    suite("CLParserSpec")(
      test("CLParser correctly parses supplied params") {
        val args   = Array("--config", cFile)
        val parser = clParser.parseOptions
        val result = parser.parse(args, ArgsConfig("defaultPath"))
        assert(result)(equalTo(Some(ArgsConfig(confFile = cFile))))
      },
      test("CLParser returns None if invalid config file is supplied") {
        val invalidConfFile = "/invalid.conf"
        val args            = Array("--config", invalidConfFile)
        val parser          = clParser.parseOptions
        val result          = parser.parse(args, ArgsConfig("defaultPath"))
        assert(result)(equalTo(None))
      }
    )
}
