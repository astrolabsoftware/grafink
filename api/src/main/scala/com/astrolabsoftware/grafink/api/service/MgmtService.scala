package com.astrolabsoftware.grafink.api.service

import scala.collection.JavaConverters._

import io.circe.generic.auto._
import org.http4s.HttpRoutes
import org.http4s.circe.CirceEntityCodec._
import org.http4s.dsl.Http4sDsl
import org.janusgraph.core.EdgeLabel
import zio._
import zio.interop.catz._
import zio.logging.Logging

import com.astrolabsoftware.grafink.JanusGraphEnv
import com.astrolabsoftware.grafink.api.service.JanusGraphConnectionManager.JanusGraphConnManagerService
import com.astrolabsoftware.grafink.models.{ InfoRequest, InfoResponse, JanusGraphConfig }

final case class MgmtService[R <: JanusGraphConnManagerService with Logging](janusGraphConfig: JanusGraphConfig) {

  type GraphTask[A] = RIO[R, A]

  val dsl = new Http4sDsl[GraphTask] {}
  import dsl._

  def routes: HttpRoutes[GraphTask] =
    HttpRoutes.of[GraphTask] {
      case req @ POST -> Root / "info" =>
        req.decode[InfoRequest] { request =>
          val response = for {
            graph <- JanusGraphConnectionManager.getOrCreateGraphInstance(request.tableName)(
              JanusGraphEnv.withHBaseStorageRead(janusGraphConfig)
            )
            mgmt = graph.openManagement()
            vertexLabels <- ZIO.effect(mgmt.getVertexLabels.asScala.toList.map(n => n.name))
            edgeLabels   <- ZIO.effect(mgmt.getRelationTypes(classOf[EdgeLabel]).asScala.toList.map(l => l.name))
            _            <- ZIO.effect(mgmt.commit())
          } yield InfoResponse(vertexLabels, edgeLabels)
          response.catchAll(t => ZIO.succeed(InfoResponse(List.empty, List.empty, error = s"$t")))
          Ok(response)
        }
    }
}
