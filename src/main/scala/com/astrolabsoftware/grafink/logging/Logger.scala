package com.astrolabsoftware.grafink.logging

import zio.{ ULayer, URLayer }
import zio.clock.Clock
import zio.console.Console
import zio.logging.{ LogAnnotation, Logging }
import zio.logging.slf4j.Slf4jLogger

object Logger {

  private val logFormat = "[correlation-id = %s] %s"

  val live: ULayer[Logging] = Slf4jLogger.make { (context, message) =>
    val correlationId = LogAnnotation.CorrelationId.render(
      context.get(LogAnnotation.CorrelationId)
    )
    logFormat.format(correlationId, message)
  }

  val test: URLayer[Console with Clock, Logging] = Logging.console()
}
