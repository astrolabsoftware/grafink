package com.astrolabsoftware.grafink.api

import io.circe.generic.auto._
import org.http4s.HttpRoutes
import org.http4s.circe.CirceEntityCodec._
import org.http4s.dsl.Http4sDsl
import zio._
import zio.interop.catz._
import zio.logging.Logging

import com.astrolabsoftware.grafink.JanusGraphEnv
import com.astrolabsoftware.grafink.api.service.{ InfoService, JanusGraphConnectionManager }
import com.astrolabsoftware.grafink.api.service.InfoService.InfoService
import com.astrolabsoftware.grafink.api.service.JanusGraphConnectionManager.JanusGraphConnManagerService
import com.astrolabsoftware.grafink.models._

final case class MgmtApi[R <: InfoService with JanusGraphConnManagerService with Logging](
  janusGraphConfig: JanusGraphConfig
) {

  type GraphTask[A] = RIO[R, A]

  val dsl = new Http4sDsl[GraphTask] {}
  import dsl._

  val emptySchemaInfo = SchemaInfo(
    vertexLabels = List.empty,
    edgeLabels = List.empty,
    propertyKeys = List.empty,
    vertexIndexes = List.empty,
    edgeIndexes = List.empty,
    relationIndexes = List.empty
  )

  def routes: HttpRoutes[GraphTask] =
    HttpRoutes.of[GraphTask] {
      case req @ POST -> Root / "info" =>
        req.decode[InfoRequest] { request =>
          val config = // Use table name from request
            janusGraphConfig.copy(storage = janusGraphConfig.storage.copy(tableName = request.tableName))
          val response = (for {
            graph  <- JanusGraphConnectionManager.getOrCreateGraphInstance(config)(JanusGraphEnv.withHBaseStorageRead)
            schema <- InfoService.getGraphInfo(graph)
          } yield {
            InfoResponse(schema = schema)
          }) catchAll (t => ZIO.succeed(InfoResponse(emptySchemaInfo, error = s"$t")))
          Ok(response)
        }
    }
}
