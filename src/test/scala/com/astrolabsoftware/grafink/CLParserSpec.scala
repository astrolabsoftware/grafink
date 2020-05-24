package com.astrolabsoftware.grafink

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class CLParserSpec extends AnyFunSuite with Matchers {

  val clParser = new CLParser {}

  test("CLParser correctly parses supplied configFile param") {
    val cFile = getClass.getResource("/application.conf").getPath
    val args  = Array("--config", cFile)

    val parser = clParser.parseOptions
    val result = parser.parse(args, ArgsConfig("defaultPath"))

    result should equal(Some(ArgsConfig(confFile = cFile)))
  }
}
