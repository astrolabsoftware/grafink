package com.astrolabsoftware.grafink.processor.edgerules

import fastparse._
import fastparse.NoWhitespace._
import fastparse.Parsed.{ Failure, Success }
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

import com.astrolabsoftware.grafink.models.GrafinkException.BadSimilarityExpression

/**
 * Parses the similarity expression for computing similarity amongst alerts
 */
object SimilarityExpParser {

  case class ParseResult(condition: Column, columns: List[String])

  /**
   * Given column name from the similarity expression
   * returns the join condition for that column after appending
   * 1, 2 to the base name
   * For eg: rfscore will return (col("rfscore1") > 0.9) && (col(rfscore2) > 0.9)
   * @param name
   * @return
   */
  def colNameToCondition(name: String): Column = {
    val col1 = s"${name}1"
    val col2 = s"${name}2"
    if (fieldToSimilarityCondtitionMap.contains(name)) {
      (fieldToSimilarityCondtitionMap(name)(col(col1), col(col2)))
    } else if (name == "mulens") {
      // Special handling for mulens
      val class1 = s"${name}_class_1"
      val class2 = s"${name}_class_2"
      mulensmlCond(
        col(s"${class1}1"),
        col(s"${class2}1"),
        col(s"${class1}2"),
        col(s"${class2}2")
      )
    } else {
      col(col1) <=> col(col2)
    }
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

  /**
   * A parser implementation using fastparse for parsing similarity expressions like
   * * (rfscore AND snnscore) OR mulens
   */
  // TODO: Wrap it over ZIO and handle the failure cases
  def parse(expr: String): ParseResult =
    fastparse.parse(expr, parser(_)) match {
      case Success(value, _) => value
      case Failure(error, index, extra) =>
        val longAggMsg = extra.trace().longAggregateMsg
        throw BadSimilarityExpression(longAggMsg)
    }

  val scoreCond: (Column, Column) => Column = (score1, score2) => (score1 > 0.9) && (score2 > 0.9)
  val cdsxmatchCond: (Column, Column) => Column = (cdsxmatch1, cdsxmatch2) =>
    (cdsxmatch1 =!= "Unknown") && (cdsxmatch1 === cdsxmatch2)
  val roidCond: (Column, Column) => Column = (roid1, roid2) => (roid1 > 1) && (roid2 > 1)
  val classtarCond: (Column, Column) => Column = (classtar1, classtar2) =>
    ((classtar1 > 0.9) && (classtar2 > 0.9)) || ((classtar1 < 0.1) && (classtar2 < 0.1))

  val mulensmlCond: (Column, Column, Column, Column) => Column =
    (mulens1_class_1, mulens1_class_2, mulens2_class_1, mulens2_class_2) =>
      (mulens1_class_1 === "ML" && mulens1_class_2 === "ML") && (mulens2_class_1 === "ML" && mulens2_class_2 === "ML")

  /**
   * Map from column base name to similarity condition
   */
  val fieldToSimilarityCondtitionMap: Map[String, (Column, Column) => Column] = Map(
    "snnscore"  -> scoreCond,
    "rfscore"   -> scoreCond,
    "cdsxmatch" -> cdsxmatchCond,
    "roid"      -> roidCond,
    "classtar"  -> classtarCond
  )
}
