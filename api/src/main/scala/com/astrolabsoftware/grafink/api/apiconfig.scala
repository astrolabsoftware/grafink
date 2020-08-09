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
package com.astrolabsoftware.grafink.api

import pureconfig._
import pureconfig.ConfigSource
import pureconfig.generic.ProductHint
import pureconfig.generic.auto._
import pureconfig.generic.semiauto._
import zio.{ Has, Task, URIO, ZIO, ZLayer }

import com.astrolabsoftware.grafink.models.{ AppConfig, GrafinkApiConfiguration, JanusGraphConfig }

package object apiconfig {

  // For pure config to be able to use camel case
  implicit def hint[T]: ProductHint[T] = ProductHint[T](ConfigFieldMapping(CamelCase, CamelCase))

  type ApiConfig = Has[AppConfig] with Has[JanusGraphConfig]

  object APIConfig {
    val live: String => ZLayer[Any, Throwable, ApiConfig] = confFile =>
      ZLayer.fromEffectMany(
        Task
          .effect(ConfigSource.file(confFile).loadOrThrow[GrafinkApiConfiguration])
          .mapError(failures => new IllegalStateException(s"Error loading configuration: $failures"))
          .map(c => Has(c.app) ++ Has(c.janusgraph))
      )

    val appConfig: URIO[Has[AppConfig], AppConfig] = ZIO.service
  }
}
