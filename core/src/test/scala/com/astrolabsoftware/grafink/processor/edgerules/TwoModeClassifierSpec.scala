package com.astrolabsoftware.grafink.processor.edgerules

import org.apache.spark.sql.Row
import zio.test._
import zio.test.Assertion._

import com.astrolabsoftware.grafink.models._
import com.astrolabsoftware.grafink.models.GrafinkException.MissingFixedVertex
import com.astrolabsoftware.grafink.processor.edgerules.SimilarityClassifierSpec._
import com.astrolabsoftware.grafink.utils.SparkTestEnv

object TwoModeClassifierSpec extends DefaultRunnableSpec {

  val sparkLayer = SparkTestEnv.test

  def spec: ZSpec[Environment, Failure] =
    suite("TwoModeClassifierSpec")(
      testM("TwoMode Classifier will correctly make an edge between fixed and new vertices") {
        val twoModeSimilarityConfig = TwoModeSimilarityConfig(recipes = List("supernova", "microlensing", "asteroids"))

        val similarityRecipes = List(
          FixedVertex(1, "similarity", List(FixedVertexProperty("recipe", "string", "supernova"))),
          FixedVertex(2, "similarity", List(FixedVertexProperty("recipe", "string", "microlensing"))),
          FixedVertex(3, "similarity", List(FixedVertexProperty("recipe", "string", "asteroids")))
        )

        val classifier = new TwoModeClassifier(twoModeSimilarityConfig, similarityRecipes)
        val currentData =
          List(
            genAlert(
              _id = 10L,
              _objectId = "ZTF19acmbyav",
              _rfscore = 0.92,
              _snn_snia_vs_nonia = 0.95,
              _classtar = 0.0,
              _roid = 1,
              _cdsxmatch = WDStar,
              _mulens_class_1 = MULENS_NULL,
              _mulens_class_2 = MULENS_NULL
            ),
            genAlert(
              _id = 12L,
              _objectId = "ZTF19acmbyap",
              _rfscore = 0.35,
              _snn_snia_vs_nonia = 0.8999,
              _classtar = 0.0,
              _roid = 1,
              _cdsxmatch = WDStar,
              _mulens_class_1 = ML,
              _mulens_class_2 = ML
            ),
            genAlert(
              _id = 13L,
              _objectId = "ZTF19acmbyap",
              _rfscore = 0.35,
              _snn_snia_vs_nonia = 0.8999,
              _classtar = 0.0,
              _roid = 2,
              _cdsxmatch = WDStar,
              _mulens_class_1 = ML,
              _mulens_class_2 = ML
            )
          )

        val app = for {
          spark <- SparkTestEnv.sparkEnv
        } yield {
          import spark.implicits._
          val loadedDf  = spark.emptyDataFrame
          val currentDf = currentData.toDF
          classifier.classify(loadedDf, currentDf).collect.toList
        }

        assertM(app.provideLayer(sparkLayer))(
          hasSameElementsDistinct(List(Row(10L, 1L, 0.0), Row(12L, 2L, 0.0), Row(13L, 2L, 0.0), Row(13L, 3L, 0.0)))
        )
      },
      testM("TwoMode Classifier will correctly make an edge between fixed and new vertices for catalog") {
        val twoModeSimilarityConfig = TwoModeSimilarityConfig(recipes = List("catalog"))

        val similarityRecipes = List(
          FixedVertex(
            1,
            "similarity",
            List(
              FixedVertexProperty("recipe", "string", "catalog"),
              FixedVertexProperty("equals", "string", EBStar.name)
            )
          ),
          FixedVertex(
            4,
            "similarity",
            List(FixedVertexProperty("recipe", "string", "catalog"), FixedVertexProperty("equals", "string", AGN.name))
          )
        )

        val classifer = new TwoModeClassifier(twoModeSimilarityConfig, similarityRecipes)
        val currentData =
          List(
            genAlert(
              _id = 57424640L,
              _objectId = "ZTF18aaacpwn",
              _rfscore = 0.374,
              _snn_snia_vs_nonia = 0.8764792680740356,
              _classtar = 1.0,
              _roid = 0,
              _cdsxmatch = EBStar,
              _mulens_class_1 = MULENS_NULL,
              _mulens_class_2 = VARIABLE
            ),
            genAlert(
              _id = 68062208L,
              _objectId = "ZTF18aaacpwn",
              _rfscore = 0.374,
              _snn_snia_vs_nonia = 0.7637328505516052,
              _classtar = 0.725,
              _roid = 0,
              _cdsxmatch = EBStar,
              _mulens_class_1 = MULENS_NULL,
              _mulens_class_2 = VARIABLE
            ),
            genAlert(
              _id = 80266496L,
              _objectId = "ZTF18aaacpwn",
              _rfscore = 0.379,
              _snn_snia_vs_nonia = 0.8499873280525208,
              _classtar = 0.978,
              _roid = 0,
              _cdsxmatch = EBStar,
              _mulens_class_1 = VARIABLE,
              _mulens_class_2 = VARIABLE
            ),
            genAlert(
              _id = 80283904L,
              _objectId = "ZTF18aaacpwn",
              _rfscore = 0.375,
              _snn_snia_vs_nonia = 0.8499873280525208,
              _classtar = 0.994,
              _roid = 0,
              _cdsxmatch = EBStar,
              _mulens_class_1 = VARIABLE,
              _mulens_class_2 = MULENS_NULL
            ),
            genAlert(
              _id = 88313856L,
              _objectId = "ZTF18aaacpwn",
              _rfscore = 0.379,
              _snn_snia_vs_nonia = 0.8325337171554565,
              _classtar = 1.0,
              _roid = 0,
              _cdsxmatch = EBStar,
              _mulens_class_1 = VARIABLE,
              _mulens_class_2 = ML
            )
          )

        val app = for {
          spark <- SparkTestEnv.sparkEnv
        } yield {
          import spark.implicits._
          val loadedDf  = spark.emptyDataFrame
          val currentDf = currentData.toDF
          classifer.classify(loadedDf, currentDf).collect.toList
        }

        assertM(app.provideLayer(sparkLayer))(
          hasSameElementsDistinct(List(
            Row(57424640L, 1L, 0.0),
            Row(68062208L, 1L, 0.0),
            Row(80266496L, 1L, 0.0),
            Row(80283904L, 1L, 0.0),
            Row(88313856L, 1L, 0.0)))
        )
      },
      testM("TwoMode Classifier will throw if a rule is configured but fixed vertex does not exist for it") {
        val twoModeSimilarityConfig = TwoModeSimilarityConfig(recipes = List("supernova", "microlensing"))

        val similarityRecipes = List(
          FixedVertex(1, "similarity", List(FixedVertexProperty("recipe", "string", "supernova")))
        )

        val classifer = new TwoModeClassifier(twoModeSimilarityConfig, similarityRecipes)
        val currentData =
          List(
            genAlert(
              _id = 10L,
              _objectId = "ZTF19acmbyav",
              _rfscore = 0.92,
              _snn_snia_vs_nonia = 0.95,
              _classtar = 0.0,
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
          val loadedDf  = spark.emptyDataFrame
          val currentDf = currentData.toDF
          classifer.classify(loadedDf, currentDf).collect.toList
        }

        assertM(app.provideLayer(sparkLayer).run)(
          dies(equalTo(MissingFixedVertex("No fixed vertex found in csv for configured rule microlensing")))
        )
      }
    )
}
