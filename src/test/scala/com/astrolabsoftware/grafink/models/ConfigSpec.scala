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
package com.astrolabsoftware.grafink.models

import java.io.File

import com.typesafe.config.ConfigFactory
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import pureconfig._
import pureconfig.generic.ProductHint
import pureconfig.generic.auto._

class ConfigSpec extends AnyFunSuite with Matchers with EitherValues {

  // For pure config to be able to use camel case
  implicit def hint[T]: ProductHint[T] = ProductHint[T](ConfigFieldMapping(CamelCase, CamelCase))

  test("ReaderConfig is parsed correctly") {
    val path = getClass.getResource("/application.conf").getPath
    val conf = ConfigFactory.parseFile(new File(path))

    val readerConfig = ConfigSource.fromConfig(conf).at("reader").load[ReaderConfig]
    readerConfig.right.value.basePath should be ("/test/base/path")
  }
}
