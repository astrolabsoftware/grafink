package com.astrolabsoftware.grafink.api.service

import scala.collection.JavaConverters._

import io.circe.{ Decoder, Encoder }
import io.circe.generic.auto._
import org.http4s.{ EntityDecoder, EntityEncoder, HttpRoutes }
import org.http4s.circe.{ jsonEncoderOf, jsonOf }
import org.http4s.circe.CirceEntityCodec._
import org.http4s.dsl.Http4sDsl
import zio._
import zio.interop.catz._
import zio.logging.Logging

import com.astrolabsoftware.grafink.JanusGraphEnv
import com.astrolabsoftware.grafink.api.service.JanusGraphConnectionManager.JanusGraphConnManagerService
import com.astrolabsoftware.grafink.models.{ InfoRequest, InfoResponse, JanusGraphConfig }

object MgmtService {

  def routes[R <: JanusGraphConnManagerService with Logging](
    janusGraphConfig: JanusGraphConfig
  ): HttpRoutes[RIO[R, *]] = {
    type InfoTask[A] = RIO[R, A]

    val dsl = new Http4sDsl[InfoTask] {}
    import dsl._

    /* implicit def circeJsonDecoder[A](
                                      implicit
                                      decoder: Decoder[A]
                                    ): EntityDecoder[InfoTask, A] =
      jsonOf[InfoTask, A]
    implicit def circeJsonEncoder[A](
                                      implicit
                                      encoder: Encoder[A]
                                    ): EntityEncoder[InfoTask, A] =
      jsonEncoderOf[InfoTask, A] */

    HttpRoutes.of[InfoTask] {
      case req @ POST -> Root / "info" =>
        req.decode[InfoRequest] { request =>
          val response = for {
            graph <- JanusGraphConnectionManager.getOrCreateGraphInstance(request.tableName)(
              JanusGraphEnv.withHBaseStorageRead(janusGraphConfig)
            )
            mgmt         = graph.openManagement()
            vertexLabels = mgmt.getVertexLabels.asScala.toList.map(n => n.name)
          } yield InfoResponse(vertexLabels)
          response.catchAll(t => ZIO.succeed(InfoResponse(List.empty, error = Some(s"$t"))))
          Ok(response)
        }
    }
  }
}
