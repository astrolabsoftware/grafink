package com.astrolabsoftware.grafink.processor.edgerules

import fastparse._
import fastparse.NoWhitespace._
import fastparse.Parsed.{ Failure, Success }
import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

import com.astrolabsoftware.grafink.models.GrafinkException.BadSimilarityExpression

object SimilarityExpParser {

  case class ParseResult(condition: Column, columns: List[String])

  def colNameToCondition(name: String, prefix1: String = "", prefix2: String = ""): Column =
    if (fieldToUdfEqualityMap.contains(name)) {
      // This column requires udf matching instead of normal equality operator
      val udf = fieldToUdfEqualityMap(name)
      if (name == "mulens") {
        // Special handling for mulens
        val class1 = s"${name}_class_1"
        val class2 = s"${name}_class_2"
        udf(
          col(s"$prefix1${class1}1"),
          col(s"$prefix1${class2}1"),
          col(s"$prefix2${class1}2"),
          col(s"$prefix2${class2}2")
        ) === lit(true)
      } else {
        udf(col(s"$prefix1${name}1"), col(s"$prefix2${name}2")) === lit(true)
      }
    } else {
      col(s"$prefix1${name}1") <=> col(s"$prefix2${name}2")
    }

  def colName[_: P]: P[ParseResult] =
    P(
      CharIn("a-zA-Z")
    ).rep(1).!.map(name => ParseResult(colNameToCondition(name), columns = List(name)))

  def ws[_: P]: P[Unit] = P(" ".rep(1))

  def operator[_: P]: P[String] = P("AND" | "OR").!

  def parens[_: P]: P[ParseResult] = P("(" ~/ parsechain ~ ")")

  def expr[_: P]: P[ParseResult] = P(colName | parens)

  def parsechain[_: P]: P[ParseResult] = P(expr ~ (ws ~ operator ~ ws ~/ expr).rep).map {
    case (init, seq) =>
      seq.foldLeft(init) {
        case (acc, (op, r)) =>
          op match {
            case "AND" => ParseResult(acc.condition && r.condition, acc.columns ++ r.columns)
            case "OR"  => ParseResult(acc.condition || r.condition, acc.columns ++ r.columns)
          }
      }
  }

  def parser[_: P]: P[ParseResult] = P(parsechain ~ End)

  // TODO: Wrap it over ZIO and handle the failure cases
  def parse(expr: String): ParseResult =
    fastparse.parse(expr, parser(_)) match {
      case Success(value, _) => value
      case Failure(error, index, extra) =>
        val longAggMsg = extra.trace().longAggregateMsg
        throw BadSimilarityExpression(longAggMsg)
    }

  val scoreudf: UserDefinedFunction =
    udf((score1: Double, score2: Double) => (score1 > 0.9) && (score2 > 0.9))
  val mulensmludf: UserDefinedFunction =
    udf { (mulens1_class_1: String, mulens1_class_2: String, mulens2_class_1: String, mulens2_class_2: String) =>
      (mulens1_class_1 == "ML" && mulens1_class_2 == "ML") && (mulens2_class_1 == "ML" && mulens2_class_2 == "ML")
    }

  val roidudf: UserDefinedFunction =
    udf((score1: Int, score2: Int) => (score1 >= 1) && (score2 >= 1))

  val classtarudf: UserDefinedFunction =
    udf((score1: Double, score2: Double) => (score1 > 0.9 && score2 > 0.9) || (score1 < 0.1 && score2 < 0.1))

  val fieldToUdfEqualityMap: Map[String, UserDefinedFunction] = Map(
    "snnscore" -> scoreudf,
    "rfscore"  -> scoreudf,
    "mulens"   -> mulensmludf,
    "roid"     -> roidudf,
    "classtar" -> classtarudf
  )
}
