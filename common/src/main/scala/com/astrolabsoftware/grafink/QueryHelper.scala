package com.astrolabsoftware.grafink

import org.janusgraph.core.JanusGraph

import com.astrolabsoftware.grafink.models.GrafinkJanusGraphConfig

object QueryHelper {

  def getGraph(janusGraphConfig: GrafinkJanusGraphConfig): JanusGraph =
    JanusGraphEnv.withHBaseStorage(janusGraphConfig)
}
