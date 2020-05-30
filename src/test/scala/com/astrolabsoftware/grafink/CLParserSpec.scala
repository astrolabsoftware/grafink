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

import zio.test._
import zio.test.Assertion._

object CLParserSpec extends DefaultRunnableSpec {

  val clParser = new CLParser {}

  def spec: ZSpec[Environment, Failure] =
    suite("CLParserSpec")(
      test("CLParser correctly parses supplied configFile param") {
        val cFile = getClass.getResource("/application.conf").getPath
        val args  = Array("--config", cFile)
        val parser = clParser.parseOptions
        val result = parser.parse(args, ArgsConfig("defaultPath"))
        assert(result)(equalTo(Some(ArgsConfig(confFile = cFile))))
      }
    )
}
