package com.astrolabsoftware.grafink.processor.edgerules

import org.apache.spark.sql.functions._
import zio.test._
import zio.test.Assertion._

import com.astrolabsoftware.grafink.processor.edgerules.SimilarityExpParser._

object SimilarityExpParserSpec extends DefaultRunnableSpec {

  def spec: ZSpec[Environment, Failure] =
    suite("SimilarityExpParserSpec")(
      test("Similarity expression parser will correctly parse given expression part I") {

        val exp    = "(rfscore AND snnscore) OR cdsxmatch"
        val result = SimilarityExpParser.parse(exp)
        assert(result)(
          equalTo(
            ParseResult(
              (scoreudf(col("rfscore1"), col("rfscore2")) === lit(true)) &&
                (scoreudf(col("snnscore1"), col("snnscore2")) === lit(true)) || (col("cdsxmatch1") <=> col(
                "cdsxmatch2"
              )),
              List("rfscore", "snnscore", "cdsxmatch")
            )
          )
        )
      },
      test("Similarity expression parser will correctly parse given expression part II") {
        val exp    = "mulens OR classtar OR roid OR cdsxmatch OR objectId"
        val result = SimilarityExpParser.parse(exp)
        assert(result)(
          equalTo(
            ParseResult(
              (mulensmludf(
                col("mulens_class_11"),
                col("mulens_class_12"),
                col("mulens_class_21"),
                col("mulens_class_22")
              ) === lit(true)) ||
                (classtarudf(col("classtar1"), col("classtar2")) === lit(true)) ||
                (col("roid1") <=> col("roid2")) || (col("cdsxmatch1") <=> col("cdsxmatch2")) || (col("objectId1") <=> col(
                "objectId2"
              )),
              List("mulens", "classtar", "roid", "cdsxmatch", "objectId")
            )
          )
        )
      },
      test("Similarity expression parser will correctly parse given expression part III") {
        val exp    = "(rfscore AND snnscore) OR (mulens OR classtar OR roid OR cdsxmatch OR objectId)"
        val result = SimilarityExpParser.parse(exp)
        assert(result)(
          equalTo(
            ParseResult(
              (scoreudf(col("rfscore1"), col("rfscore2")) === lit(true)) &&
                (scoreudf(col("snnscore1"), col("snnscore2")) === lit(true)) ||
                ((mulensmludf(
                  col("mulens_class_11"),
                  col("mulens_class_12"),
                  col("mulens_class_21"),
                  col("mulens_class_22")
                ) === lit(true)) ||
                  (classtarudf(col("classtar1"), col("classtar2")) === lit(true)) ||
                  (col("roid1") <=> col("roid2")) || (col("cdsxmatch1") <=> col("cdsxmatch2")) || (col("objectId1") <=> col(
                  "objectId2"
                ))),
              List("rfscore", "snnscore", "mulens", "classtar", "roid", "cdsxmatch", "objectId")
            )
          )
        )
      }
    )
}
