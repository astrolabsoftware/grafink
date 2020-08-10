package com.astrolabsoftware.grafink.processor.edgerules

import org.apache.spark.sql.functions._
import zio.test._
import zio.test.Assertion._

import com.astrolabsoftware.grafink.processor.edgerules.SimilarityExpParser._

object SimilarityExpParserSpec extends DefaultRunnableSpec {

  def spec: ZSpec[Environment, Failure] =
    suite("SimilarityExpParserSpec")(
      test("Similarity expression parser will correctly parse given expression part I") {

        val exp    = "(rfscore AND snnscore) OR objectId"
        val result = SimilarityExpParser.parse(exp)
        assert(result)(
          equalTo(
            ParseResult(
              (((col("rfscore1") > 0.9) && (col("rfscore2") > 0.9)) &&
                ((col("snnscore1") > 0.9) && (col("snnscore2") > 0.9))) || (col("objectId1") <=> col(
                "objectId2"
              )),
              List("rfscore", "snnscore", "objectId")
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
              (((col("mulens_class_11") === "ML") && (col("mulens_class_21") === "ML")) &&
                ((col("mulens_class_12") === "ML") && (col("mulens_class_22") === "ML"))) ||
                (((col("classtar1") > 0.9) && (col("classtar2") > 0.9)) || ((col("classtar1") < 0.1) && (col(
                  "classtar2"
                ) < 0.1))) ||
                ((col("roid1") > 1) && (col("roid2") > 1)) ||
                ((col("cdsxmatch1") =!= "Unknown") && (col("cdsxmatch1") === col("cdsxmatch2"))) ||
                (col("objectId1") <=> col("objectId2")),
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
              (((col("rfscore1") > 0.9) && (col("rfscore2") > 0.9)) &&
                ((col("snnscore1") > 0.9) && (col("snnscore2") > 0.9))) ||
                ((((col("mulens_class_11") === "ML") && (col("mulens_class_21") === "ML")) &&
                  ((col("mulens_class_12") === "ML") && (col("mulens_class_22") === "ML"))) ||
                  (((col("classtar1") > 0.9) && (col("classtar2") > 0.9)) || ((col("classtar1") < 0.1) && (col(
                    "classtar2"
                  ) < 0.1))) ||
                  ((col("roid1") > 1) && (col("roid2") > 1)) ||
                  ((col("cdsxmatch1") =!= "Unknown") && (col("cdsxmatch1") === col("cdsxmatch2"))) ||
                  (col("objectId1") <=> col("objectId2"))),
              List("rfscore", "snnscore", "mulens", "classtar", "roid", "cdsxmatch", "objectId")
            )
          )
        )
      }
    )
}
