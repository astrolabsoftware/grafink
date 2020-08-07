package com.astrolabsoftware.grafink.api.service

import org.apache.hadoop.hbase.TableNotEnabledException
import org.janusgraph.core.JanusGraph
import zio._
import zio.logging.{ log, Logging }

import com.astrolabsoftware.grafink.api.cache.SimpleCache
import com.astrolabsoftware.grafink.models.GrafinkException.ConnectionLimitReachedException
import com.astrolabsoftware.grafink.models.JanusGraphConfig

object JanusGraphConnectionManager {

  type JanusGraphConnManagerService = Has[JanusGraphConnectionManager.Service]

  trait Service {

    /**
     * This will create the graph instance everytime if the cache is full
     * @param janusGraphConfig
     * @return
     */
    def getOrCreateGraphInstance(janusGraphConfig: JanusGraphConfig)(
      create: JanusGraphConfig => JanusGraph
    ): ZIO[Logging, Throwable, JanusGraph]
  }

  val live: Int => ZLayer[Logging, IllegalArgumentException, JanusGraphConnManagerService] = capacity =>
    ZLayer.fromEffect {
      SimpleCache.make(capacity).map { cache =>
        new Service {
          override def getOrCreateGraphInstance(
            janusGraphConfig: JanusGraphConfig
          )(create: JanusGraphConfig => JanusGraph): ZIO[Logging, Throwable, JanusGraph] =
            for {
              graphOption <- cache.get(janusGraphConfig.storage.tableName)
              graph <- if (graphOption.isEmpty) {
                // Create new instance and try to cache it
                for {
                  graphInstance <- ZIO.effect(create(janusGraphConfig)) tapError { e =>
                    e match {
                      case e: TableNotEnabledException =>
                        log.error(s"Table ${janusGraphConfig.storage.tableName} is not enabled")
                      case es: java.lang.IllegalArgumentException =>
                        log.error(s"Error connecting to Elasticsearch at ${janusGraphConfig.indexBackend.host}")
                      case _ => ZIO.unit
                    }
                  }
                  _ <- cache.put(janusGraphConfig.storage.tableName, graphInstance) catchSome {
                    case ConnectionLimitReachedException(e) =>
                      log.error(e)
                  }
                } yield graphInstance
              } else {
                ZIO.succeed(graphOption.get)
              }
            } yield graph
        }
      }
    }

  def getOrCreateGraphInstance(
    janusGraphConfig: JanusGraphConfig
  )(create: JanusGraphConfig => JanusGraph): RIO[JanusGraphConnManagerService with Logging, JanusGraph] =
    ZIO.accessM(_.get.getOrCreateGraphInstance(janusGraphConfig)(create))
}
