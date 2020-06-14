package com.astrolabsoftware.grafink.utils

import org.janusgraph.core.JanusGraph
import zio.ZManaged
import zio.logging.Logging

import com.astrolabsoftware.grafink.JanusGraphEnv
import com.astrolabsoftware.grafink.models.JanusGraphConfig

object JanusGraphTestEnv {
  val test: JanusGraphConfig => ZManaged[Logging, Throwable, JanusGraph] = config => JanusGraphEnv.inmemory(config)
}
