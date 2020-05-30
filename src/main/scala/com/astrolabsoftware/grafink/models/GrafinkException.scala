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

sealed abstract class GrafinkException(val error: String) extends Throwable

object GrafinkException {
  final case class BadArgumentsException(override val error: String) extends GrafinkException(error)

  def getExitCode(e: GrafinkException): zio.ExitCode = e match {
    case _: BadArgumentsException => ExitCodes.badArguments
  }
}

object ExitCodes {

  val badArguments = zio.ExitCode(2)
}
