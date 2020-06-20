package com.astrolabsoftware.grafink.models

sealed abstract class Cdsxmatch(val name: String)

case object Cdsxmatch_UNKNOWN extends Cdsxmatch("Unknown")
case object WDStar            extends Cdsxmatch("WD*")
case object CepheId           extends Cdsxmatch("Cepheid")
case object EllipVar          extends Cdsxmatch("EllipVar")
case object AGN               extends Cdsxmatch("AGN")
case object CStar             extends Cdsxmatch("C*")

sealed abstract class Mulens(val name: String)

case object MULENS_NULL extends Mulens("")
case object VARIABLE    extends Mulens("VARIABLE")
case object CV          extends Mulens("CV")
case object ML          extends Mulens("ML")
case object CONSTANT    extends Mulens("CONSTANT")

case class Candidate(jd: Double, programid: Int, candid: Long)

case class Alert(
  id: Long,
  objectId: String,
  candidate: Candidate,
  cdsxmatch: String,
  rfscore: Double,
  snnscore: Double,
  classtar: Double,
  roid: Double,
  mulens_class_1: Option[String] = None,
  mulens_class_2: Option[String] = None,
  year: Int,
  month: Int,
  day: Int
)

object Alert {}
