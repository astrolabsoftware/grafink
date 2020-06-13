package com.astrolabsoftware.grafink.utils

import org.janusgraph.core.JanusGraph
import zio.{ Has, ZIO, ZLayer }
import zio.blocking.Blocking

import com.astrolabsoftware.grafink.JanusGraphEnv
import com.astrolabsoftware.grafink.JanusGraphEnv.JanusGraphEnv
import com.astrolabsoftware.grafink.models.JanusGraphConfig

object JanusGraphTestEnv {

  val test: ZLayer[Blocking with Has[JanusGraphConfig], Throwable, JanusGraphEnv] = JanusGraphEnv.inmemory

  def graph: ZIO[JanusGraphEnv, Throwable, JanusGraph] = ZIO.access(_.get.graph)
}
