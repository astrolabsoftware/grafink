package com.astrolabsoftware.grafink

import org.janusgraph.core.JanusGraph

import com.astrolabsoftware.grafink.models.JanusGraphConfig

object QueryHelper {

  def getGraph(janusGraphConfig: JanusGraphConfig): JanusGraph =
    JanusGraphEnv.withHBaseStorage(janusGraphConfig)
}
