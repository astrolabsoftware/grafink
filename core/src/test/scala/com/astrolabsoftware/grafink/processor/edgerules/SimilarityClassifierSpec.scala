package com.astrolabsoftware.grafink.processor.edgerules

import org.apache.spark.sql.Row
import zio.test._
import zio.test.Assertion.hasSameElementsDistinct

import com.astrolabsoftware.grafink.models._
import com.astrolabsoftware.grafink.utils.SparkTestEnv

object SimilarityClassifierSpec extends DefaultRunnableSpec {

  val sparkLayer = SparkTestEnv.test

  def mulensToString(m: Mulens): Option[String] = m match {
    case MULENS_NULL => None
    case e           => Some(e.name)
  }

  // scalastyle:off
  def genAlert(
    _id: Long,
    _objectId: String,
    _rfscore: Double,
    _snn_snia_vs_nonia: Double,
    _roid: Int,
    _classtar: Double,
    _cdsxmatch: Cdsxmatch,
    _mulens_class_1: Mulens,
    _mulens_class_2: Mulens,
    _snn_sn_vs_all: Float = 0.80f,
    _drb: Float = 0.79f,
    _ndethist: Int = 392
  ): Alert =
    Alert(
      id = _id,
      objectId = _objectId,
      candidate = Candidate(0.0, 5, scala.util.Random.nextLong + 1034528862715010005L),
      cdsxmatch = _cdsxmatch.name,
      rfscore = _rfscore,
      snn_snia_vs_nonia = _snn_snia_vs_nonia,
      snn_sn_vs_all = _snn_sn_vs_all,
      drb = _drb,
      ndethist = _ndethist,
      roid = _roid,
      classtar = _classtar,
      mulens_class_1 = mulensToString(_mulens_class_1),
      mulens_class_2 = mulensToString(_mulens_class_2),
      year = 2019,
      month = 2,
      day = 1
    )
  // scalastyle:on

  def spec: ZSpec[Environment, Failure] =
    suite("SimilarityClassifierSpec")(
      testM("Similarity classifier will correctly make an edge between new and old vertices") {

        val similarityConfig =
          SimilarityConfig(similarityExp = "rfscore OR objectId")

        val similarityClassifer = new SimilarityClassifer(similarityConfig)

        val loadedData =
          List(
            genAlert(
              _id = 1L,
              _objectId = "ZTF19acmbyav",
              _rfscore = 0.388,
              _snn_snia_vs_nonia = 0.36001157760620117,
              _classtar = 0.0,
              _roid = 1,
              _cdsxmatch = WDStar,
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
              _snn_snia_vs_nonia = 0.67001157760620889,
              _classtar = 0.0,
              _roid = 0,
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

        assertM(app.provideLayer(sparkLayer))(hasSameElementsDistinct(List(Row(2L, 1L, 1))))
      },
      testM("Similarity classifier will correctly make an edge between new vertices") {

        val similarityConfig =
          SimilarityConfig(similarityExp = "rfscore OR objectId")

        val similarityClassifer = new SimilarityClassifer(similarityConfig)

        val currentData =
          List(
            genAlert(
              _id = 1L,
              _objectId = "ZTF19acmbyav",
              _rfscore = 0.388,
              _snn_snia_vs_nonia = 0.36001157760620117,
              _classtar = 0.0,
              _roid = 1,
              _cdsxmatch = WDStar,
              _mulens_class_1 = MULENS_NULL,
              _mulens_class_2 = MULENS_NULL
            ),
            genAlert(
              _id = 2L,
              _objectId = "ZTF19acmbyav",
              _rfscore = 0.988,
              _snn_snia_vs_nonia = 0.67001157760620889,
              _classtar = 0.0,
              _roid = 0,
              _cdsxmatch = Cdsxmatch_UNKNOWN,
              _mulens_class_1 = MULENS_NULL,
              _mulens_class_2 = MULENS_NULL
            )
          )

        val loadedData =
          List(
            genAlert(
              _id = 3L,
              _objectId = "ZTF20acmkyap",
              _rfscore = 0.188,
              _snn_snia_vs_nonia = 0.67001157760620889,
              _classtar = 0.2,
              _roid = 0,
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

        assertM(app.provideLayer(sparkLayer))(hasSameElementsDistinct(List(Row(2L, 1L, 1))))
      },
      testM("Similarity classifier will correctly calculate similarity") {

        val similarityConfig =
          SimilarityConfig(similarityExp =
            "(rfscore AND snn_snia_vs_nonia) OR mulens OR classtar OR cdsxmatch OR objectId OR roid"
          )

        val similarityClassifer = new SimilarityClassifer(similarityConfig)

        val loadedData =
          List(
            genAlert(
              _id = 1L,
              _objectId = "toto",
              _rfscore = 0.99,
              _snn_snia_vs_nonia = 0.8,
              _classtar = 0.0,
              _roid = 3,
              _cdsxmatch = CStar,
              _mulens_class_1 = CONSTANT,
              _mulens_class_2 = CONSTANT
            )
          )

        val currentData =
          List(
            genAlert(
              _id = 2L,
              _objectId = "toto",
              _rfscore = 0.95,
              _snn_snia_vs_nonia = 0.95,
              _classtar = 0.0,
              _roid = 2,
              _cdsxmatch = CStar,
              _mulens_class_1 = ML,
              _mulens_class_2 = CONSTANT
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

        assertM(app.provideLayer(sparkLayer))(hasSameElementsDistinct(List(Row(2L, 1L, 5))))
      }
    )
}
