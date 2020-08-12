package com.astrolabsoftware.grafink.processor.vertex

import zio.test._
import zio.test.Assertion._

import com.astrolabsoftware.grafink.logging.Logger
import com.astrolabsoftware.grafink.models._

object FixedVertexDataReaderSpec extends DefaultRunnableSpec {

  def spec: ZSpec[Environment, Failure] = suite("FixedVertexDataReaderSpec")(
    testM("FixedVertexDataReader will correctly read fixed vertex data") {
      val vertexLoaderConfig = VertexLoaderConfig(10, "alert", "/fixedvertices.csv")
      val app =
        for {
          recipes <- FixedVertexDataReader.readFixedVertexData(vertexLoaderConfig)
        } yield recipes

      val logger = Logger.test
      val layer  = (logger >>> FixedVertexDataReader.live) ++ logger

      assertM(app.provideLayer(layer))(
        hasSameElementsDistinct(
          List(
            FixedVertex(1L, "similarity", List(FixedVertexProperty("recipe", "string", "supernova"))),
            FixedVertex(2L, "similarity", List(FixedVertexProperty("recipe", "string", "microlensing"))),
            FixedVertex(3L, "similarity", List(FixedVertexProperty("recipe", "string", "asteroids"))),
            FixedVertex(
              4L,
              "similarity",
              List(
                FixedVertexProperty("recipe", "string", "catalog"),
                FixedVertexProperty("equals", "string", WDStar.name)
              )
            ),
            FixedVertex(
              5L,
              "similarity",
              List(
                FixedVertexProperty("recipe", "string", "catalog"),
                FixedVertexProperty("equals", "string", CepheId.name)
              )
            ),
            FixedVertex(
              6L,
              "similarity",
              List(
                FixedVertexProperty("recipe", "string", "catalog"),
                FixedVertexProperty("equals", "string", AGN.name)
              )
            )
          )
        )
      )
    }
  )
}
