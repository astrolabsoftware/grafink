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

/* These classes are only used for testing */
sealed abstract class Cdsxmatch(val name: String)

case object Cdsxmatch_UNKNOWN extends Cdsxmatch("Unknown")
case object WDStar            extends Cdsxmatch("WD*")
case object CepheId           extends Cdsxmatch("Cepheid")
case object EllipVar          extends Cdsxmatch("EllipVar")
case object AGN               extends Cdsxmatch("AGN")
case object CStar             extends Cdsxmatch("C*")
case object EBStar            extends Cdsxmatch("EB*")

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
    snn_snia_vs_nonia: Double,
    classtar: Double,
    roid: Int,
    mulens_class_1: Option[String] = None,
    mulens_class_2: Option[String] = None,
    year: Int,
    month: Int,
    day: Int
)

object Alert {}
