package com.astrolabsoftware.grafink.processor.edgerules

import org.apache.spark.sql.Row
import zio.test._
import zio.test.Assertion._

import com.astrolabsoftware.grafink.models._
import com.astrolabsoftware.grafink.processor.edgerules.SimilarityClassifierSpec.genAlert
import com.astrolabsoftware.grafink.utils.SparkTestEnv

object SameValueClassifierSpec extends DefaultRunnableSpec {

  val sparkLayer = SparkTestEnv.test

  def spec: ZSpec[Environment, Failure] =
    suite("SameValueClassifierSpec")(
      testM("SameValueClassifer will correctly make an edge between alerts with same value of a column") {

        val config = SameValueSimilarityConfig(List("objectId"))

        val classifier = new SameValueClassifier(config)

        val currentData =
          List(
            genAlert(
              _id = 1L,
              _objectId = "obj1",
              _rfscore = 0.92,
              _snn_snia_vs_nonia = 0.95,
              _classtar = 0.0f,
              _roid = 1,
              _cdsxmatch = WDStar,
              _mulens_class_1 = MULENS_NULL,
              _mulens_class_2 = MULENS_NULL
            ),
            genAlert(
              _id = 2L,
              _objectId = "obj2",
              _rfscore = 0.35,
              _snn_snia_vs_nonia = 0.8999,
              _classtar = 0.0f,
              _roid = 1,
              _cdsxmatch = WDStar,
              _mulens_class_1 = ML,
              _mulens_class_2 = ML
            ),
            genAlert(
              _id = 3L,
              _objectId = "obj1",
              _rfscore = 0.35,
              _snn_snia_vs_nonia = 0.8999,
              _classtar = 0.0f,
              _roid = 2,
              _cdsxmatch = WDStar,
              _mulens_class_1 = ML,
              _mulens_class_2 = ML
            )
          )

        val loadedData =
          List(
            genAlert(
              _id = 4L,
              _objectId = "obj3",
              _rfscore = 0.92,
              _snn_snia_vs_nonia = 0.95,
              _classtar = 0.0f,
              _roid = 1,
              _cdsxmatch = WDStar,
              _mulens_class_1 = MULENS_NULL,
              _mulens_class_2 = MULENS_NULL
            ),
            genAlert(
              _id = 13L,
              _objectId = "obj1",
              _rfscore = 0.92,
              _snn_snia_vs_nonia = 0.95,
              _classtar = 0.0f,
              _roid = 1,
              _cdsxmatch = WDStar,
              _mulens_class_1 = MULENS_NULL,
              _mulens_class_2 = MULENS_NULL
            ),
            genAlert(
              _id = 4L,
              _objectId = "obj5",
              _rfscore = 0.92,
              _snn_snia_vs_nonia = 0.95,
              _classtar = 0.0f,
              _roid = 1,
              _cdsxmatch = WDStar,
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
          classifier.classify(loadedDf, currentDf).collect.toList
        }

        assertM(app.provideLayer(sparkLayer))(
          hasSameElementsDistinct(List(Row(1L, 3L, "objectId"), Row(1L, 13L, "objectId"), Row(3L, 13L, "objectId")))
        )
      },
      testM(
        "SameValueClassifer will correctly make an edge between alerts with same value of a column for each configured column"
      ) {

        val config = SameValueSimilarityConfig(List("objectId", "cdsxmatch"))

        val classifier = new SameValueClassifier(config)

        val currentData =
          List(
            genAlert(
              _id = 1L,
              _objectId = "obj1",
              _rfscore = 0.92,
              _snn_snia_vs_nonia = 0.95,
              _classtar = 0.0f,
              _roid = 1,
              _cdsxmatch = WDStar,
              _mulens_class_1 = MULENS_NULL,
              _mulens_class_2 = MULENS_NULL
            ),
            genAlert(
              _id = 2L,
              _objectId = "obj2",
              _rfscore = 0.35,
              _snn_snia_vs_nonia = 0.8999,
              _classtar = 0.0f,
              _roid = 1,
              _cdsxmatch = WDStar,
              _mulens_class_1 = ML,
              _mulens_class_2 = ML
            ),
            genAlert(
              _id = 3L,
              _objectId = "obj1",
              _rfscore = 0.35,
              _snn_snia_vs_nonia = 0.8999,
              _classtar = 0.0f,
              _roid = 2,
              _cdsxmatch = AGN,
              _mulens_class_1 = ML,
              _mulens_class_2 = ML
            )
          )

        val loadedData =
          List(
            genAlert(
              _id = 4L,
              _objectId = "obj3",
              _rfscore = 0.92,
              _snn_snia_vs_nonia = 0.95,
              _classtar = 0.0f,
              _roid = 1,
              _cdsxmatch = WDStar,
              _mulens_class_1 = MULENS_NULL,
              _mulens_class_2 = MULENS_NULL
            ),
            genAlert(
              _id = 13L,
              _objectId = "obj1",
              _rfscore = 0.92,
              _snn_snia_vs_nonia = 0.95,
              _classtar = 0.0f,
              _roid = 1,
              _cdsxmatch = CepheId,
              _mulens_class_1 = MULENS_NULL,
              _mulens_class_2 = MULENS_NULL
            ),
            genAlert(
              _id = 4L,
              _objectId = "obj5",
              _rfscore = 0.92,
              _snn_snia_vs_nonia = 0.95,
              _classtar = 0.0f,
              _roid = 1,
              _cdsxmatch = CStar,
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
          classifier.classify(loadedDf, currentDf).collect.toList
        }

        assertM(app.provideLayer(sparkLayer))(
          hasSameElementsDistinct(
            List(
              Row(1L, 3L, "objectId"),
              Row(1L, 13L, "objectId"),
              Row(3L, 13L, "objectId"),
              Row(1L, 2L, "cdsxmatch"),
              Row(1L, 4L, "cdsxmatch"),
              Row(2L, 4L, "cdsxmatch")
            )
          )
        )
      }
    )

}
