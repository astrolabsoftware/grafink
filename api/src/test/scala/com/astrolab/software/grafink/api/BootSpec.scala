package com.astrolab.software.grafink.api

import zio.test._
import zio.test.Assertion._

import com.astrolabsoftware.grafink.api.{ ArgsConfig, Boot }
import com.astrolabsoftware.grafink.models.ExitCodes

object BootSpec extends DefaultRunnableSpec {

  val cFile = getClass.getResource("/application.conf").getPath

  def spec: ZSpec[Environment, Failure] =
    suite("BootSpec")(
      test("Supplied args are parsed correctly if conf file is a valid path") {
        val dateString = "2019-10-12"
        val duration   = 2
        val config     = Boot.parseArgs(List("--config", cFile))
        assert(config)(equalTo(Some(ArgsConfig(confFile = cFile))))
      },
      test("Supplied args are not parsed if supplied conf file does not exist") {
        val config = Boot.parseArgs(List("--config", "/path/does/not/exist"))
        assert(config)(equalTo(None))
      },
      testM("If incorrect args are supplied, job will fail") {
        val program = Boot.run(List("--wrongconfig", cFile))
        assertM(program.run)(succeeds(equalTo(ExitCodes.badArguments)))
      }
    )
}
