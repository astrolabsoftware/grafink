package com.astrolabsoftware.grafink.processor.edgerules

import zio.test._
import zio.test.Assertion.hasSameElementsDistinct

import com.astrolabsoftware.grafink.models._
import com.astrolabsoftware.grafink.processor.EdgeProcessor.MakeEdge
import com.astrolabsoftware.grafink.utils.SparkTestEnv

object SimilarityClassifierSpec extends DefaultRunnableSpec {

  val sparkLayer = SparkTestEnv.test

  def mulensToString(m: Mulens): Option[String] = m match {
    case MULENS_NULL => None
    case e           => Some(e.name)
  }

  def genAlert(
    _id: Long,
    _objectId: String,
    _rfscore: Double,
    _snnscore: Double,
    _cdsxmatch: Cdsxmatch,
    _mulens_class_1: Mulens,
    _mulens_class_2: Mulens
  ): Alert =
    Alert(
      id = _id,
      objectId = _objectId,
      candidate = Candidate(0.0, 5, scala.util.Random.nextLong + 1034528862715010005L),
      cdsxmatch = _cdsxmatch.name,
      rfscore = _rfscore,
      snnscore = _snnscore,
      mulens_class_1 = mulensToString(_mulens_class_1),
      mulens_class_2 = mulensToString(_mulens_class_2),
      year = 2019,
      month = 2,
      day = 1
    )

  def spec: ZSpec[Environment, Failure] =
    suite("SimilarityClassifierSpec")(
      testM("Similarity classifier will correctly make an edge between new and old vertices") {

        val similarityConfig =
          SimilarityConfig(similarityExp = "rfscore OR objectId", parallelism = 2)

        val similarityClassifer = new SimilarityClassifer(similarityConfig)

        val loadedData =
          List(
            genAlert(
              _id = 1L,
              _objectId = "ZTF19acmbyav",
              _rfscore = 0.388,
              _snnscore = 0.36001157760620117,
              _cdsxmatch = Cdsxmatch_UNKNOWN,
              _mulens_class_1 = MULENS_NULL,
              _mulens_class_2 = MULENS_NULL
            )
          )

        val currentData =
          List(
            genAlert(
              _id = 2L,
              _objectId = "ZTF19acmbyav",
              _rfscore = 0.988,
              _snnscore = 0.67001157760620889,
              _cdsxmatch = Cdsxmatch_UNKNOWN,
              _mulens_class_1 = MULENS_NULL,
              _mulens_class_2 = MULENS_NULL
            )
          )

        val app = for {
          spark <- SparkTestEnv.sparkEnv

        } yield {
          import spark.implicits._
          val loadedDf  = loadedData.toDF
          val currentDf = currentData.toDF
          similarityClassifer.classify(loadedDf, currentDf).collect.toList
        }

        assertM(app.provideLayer(sparkLayer))(hasSameElementsDistinct(List(MakeEdge(2L, 1L, "1"))))
      }
    )
}
