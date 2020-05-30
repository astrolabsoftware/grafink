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

import zio.test._
import zio.test.Assertion._

import com.astrolabsoftware.grafink.CLParser.dateFormat

object CLParserSpec extends DefaultRunnableSpec {

  val clParser = new CLParser {}
  val cFile    = getClass.getResource("/application.conf").getPath

  def spec: ZSpec[Environment, Failure] =
    suite("CLParserSpec")(
      test("CLParser correctly parses supplied params") {
        val dateString = "2019-10-12"
        val date       = LocalDate.parse(dateString, dateFormat)
        val duration   = 2
        val args       = Array("--config", cFile, "--startdate", dateString, "--duration", s"$duration")
        val parser     = clParser.parseOptions
        val result     = parser.parse(args, ArgsConfig("defaultPath", LocalDate.now, 1))
        assert(result)(equalTo(Some(ArgsConfig(confFile = cFile, startDate = date, duration = duration))))
      },
      test("CLParser returns None if invalid startDate is supplied") {
        val dateString = "2019-13-12"
        val duration   = 2
        val args       = Array("--config", cFile, "--startdate", dateString, "--duration", s"$duration")
        val parser     = clParser.parseOptions
        val result     = parser.parse(args, ArgsConfig("defaultPath", LocalDate.now, 1))
        assert(result)(equalTo(None))
      }
    )
}
