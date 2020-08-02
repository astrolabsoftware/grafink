package com.astrolabsoftware.grafink.api.service

import org.apache.hadoop.hbase.TableNotEnabledException
import org.janusgraph.core.JanusGraph
import zio._
import zio.logging.{ log, Logging }

import com.astrolabsoftware.grafink.api.cache.SimpleCache
import com.astrolabsoftware.grafink.models.GrafinkException.ConnectionLimitReachedException

object JanusGraphConnectionManager {

  type JanusGraphConnManagerService = Has[JanusGraphConnectionManager.Service]

  trait Service {

    /**
     * This will create the graph instance everytime if the cache is full
     * @param tableName
     * @return
     */
    def getOrCreateGraphInstance(tableName: String)(create: => JanusGraph): ZIO[Logging, Throwable, JanusGraph]
  }

  val live: Int => ZLayer[Logging, IllegalArgumentException, JanusGraphConnManagerService] = capacity =>
    ZLayer.fromEffect {
      SimpleCache.make(capacity).map { cache =>
        new Service {
          override def getOrCreateGraphInstance(
            tableName: String
          )(create: => JanusGraph): ZIO[Logging, Throwable, JanusGraph] =
            for {
              graphOption <- cache.get(tableName)
              graph <- if (graphOption.isEmpty) {
                // Create new instance and try to cache it
                for {
                  graphInstance <- ZIO.effect(create) tapError {
                    case e: TableNotEnabledException => log.error(s"Table $tableName is not enabled")
                  }
                  _ <- cache.put(tableName, graphInstance) catchSome {
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
    tableName: String
  )(create: => JanusGraph): RIO[JanusGraphConnManagerService with Logging, JanusGraph] =
    ZIO.accessM(_.get.getOrCreateGraphInstance(tableName)(create))
}
