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

        val classifer = new TwoModeClassifier(twoModeSimilarityConfig, similarityRecipes)
        val currentData =
          List(
            genAlert(
              _id = 10L,
              _objectId = "ZTF19acmbyav",
              _rfscore = 0.92,
              _snnscore = 0.95,
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
              _snnscore = 0.8999,
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
              _snnscore = 0.8999,
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
          classifer.classify(loadedDf, currentDf).collect.toList
        }

        assertM(app.provideLayer(sparkLayer))(
          hasSameElementsDistinct(List(Row(10L, 1L, 0.0), Row(12L, 2L, 0.0), Row(13L, 2L, 0.0), Row(13L, 3L, 0.0)))
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
              _snnscore = 0.95,
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
