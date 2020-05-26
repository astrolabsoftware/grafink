package com.astrolabsoftware.grafink

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class BootSpec extends AnyFunSuite with Matchers {

  test("Supplied args are parsed correctly if conf file is a valid path") {
    val cFile = getClass.getResource("/application.conf").getPath
    val config = Boot.parseArgs(Array("--config", cFile))
    config should equal(Some(ArgsConfig(confFile = cFile)))
  }

  test("Supplied args are not parsed if supplied conf file does not exist") {
    val config = Boot.parseArgs(Array("--config", "/path/does/not/exist"))
    config should equal(None)
  }
}
