package com.astrolabsoftware.grafink.api

import pureconfig._
import pureconfig.ConfigSource
import pureconfig.generic.auto._
import pureconfig.generic.semiauto._
import pureconfig.generic.ProductHint
import zio.{ Has, Task, ZLayer }

import com.astrolabsoftware.grafink.models.GrafinkApiConfiguration

package object apiconfig {

  // For pure config to be able to use camel case
  implicit def hint[T]: ProductHint[T] = ProductHint[T](ConfigFieldMapping(CamelCase, CamelCase))

  type ApiConfig = Has[GrafinkApiConfiguration]

  object APIConfig {
    val live: String => ZLayer[Any, Throwable, ApiConfig] = confFile =>
      ZLayer.fromEffect(
        Task
          .effect(ConfigSource.file(confFile).loadOrThrow[GrafinkApiConfiguration])
          .mapError(failures => new IllegalStateException(s"Error loading configuration: $failures"))
      )
  }
}
