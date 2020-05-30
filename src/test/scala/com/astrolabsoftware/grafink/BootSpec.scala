package com.astrolabsoftware.grafink

import zio.test._
import zio.test.Assertion._

object BootSpec extends DefaultRunnableSpec {

  def spec: ZSpec[Environment, Failure] =
    suite("BootSpec")(
      test("Supplied args are parsed correctly if conf file is a valid path") {
        val cFile = getClass.getResource("/application.conf").getPath
        val config = Boot.parseArgs(Array("--config", cFile))
        assert(config)(equalTo(Some(ArgsConfig(confFile = cFile))))
      },
      test("Supplied args are not parsed if supplied conf file does not exist") {
        val config = Boot.parseArgs(Array("--config", "/path/does/not/exist"))
        assert(config)(equalTo(None))
      }
    )
}
