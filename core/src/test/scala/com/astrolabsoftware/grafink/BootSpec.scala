package com.astrolabsoftware.grafink

import java.time.LocalDate

import zio.test._
import zio.test.Assertion._

import com.astrolabsoftware.grafink.common.PartitionManager.dateFormat
import com.astrolabsoftware.grafink.models.ExitCodes

object BootSpec extends DefaultRunnableSpec {

  val cFile = getClass.getResource("/application.conf").getPath

  def spec: ZSpec[Environment, Failure] =
    suite("BootSpec")(
      test("Supplied args are parsed correctly if conf file is a valid path") {
        val dateString = "2019-10-12"
        val date       = LocalDate.parse(dateString, dateFormat)
        val duration   = 2
        val config     = Boot.parseArgs(Array("--config", cFile, "--startdate", dateString, "--duration", s"$duration"))
        assert(config)(equalTo(Some(ArgsConfig(confFile = cFile, startDate = date, duration = duration))))
      },
      test("Supplied args are not parsed if supplied conf file does not exist") {
        val config = Boot.parseArgs(Array("--config", "/path/does/not/exist", "--startdate", "2019-10-12"))
        assert(config)(equalTo(None))
      },
      testM("If incorrect args are supplied, job will fail") {
        val program = Boot.run(List("--wrongconfig", cFile))
        assertM(program.run)(succeeds(equalTo(ExitCodes.badArguments)))
      },
      test("Supplied args are parsed correctly even if duration is not supplied") {
        val dateString = "2019-10-12"
        val date       = LocalDate.parse(dateString, dateFormat)
        val config     = Boot.parseArgs(Array("--config", cFile, "--startdate", dateString))
        assert(config)(equalTo(Some(ArgsConfig(confFile = cFile, startDate = date, duration = 1))))
      }
    )
}
