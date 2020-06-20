/*
 * Copyright 2020 AstroLab Software
 * Author: Yash Datta
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.astrolabsoftware.grafink.processor.edgerules

import org.apache.spark.sql.{ Column, DataFrame, Dataset }
import org.apache.spark.sql.functions._

import com.astrolabsoftware.grafink.models.SimilarityConfig
import com.astrolabsoftware.grafink.processor.EdgeProcessor.MakeEdge

class SimilarityClassifer(config: SimilarityConfig) extends VertexClassifierRule {

  override def name: String = "similarityClassifier"

  /* def parseToJoinCondition(exp: String, cols: List[String]): (Column, List[String]) = {
    SimilarityExpParser.parseExpression(exp) match {
      case head :: Nil => (col(head), head :: cols)
      case head :: tail =>
        val one = parseToJoinCondition(head.substring(1, head.length - 1), cols)
        val two = parseToJoinCondition(tail, cols)

    }
  } */

  // def getColumnNamesFromExpr(expr: String, cols: List[String])

  /**
   * Given a loaded data df (existing data in janusgraph), and current data df, return
   * an RDD[{@link MakeEdge}]
   *
   * @param df
   * @return
   */
  override def classify(loadedDf: DataFrame, df: DataFrame): Dataset[MakeEdge] = {

    val similarityExpression = config.similarityExp // """(rfscore AND snnscore) OR classtar"""
    val parsed               = SimilarityExpParser.parse(similarityExpression)
    // TODO: Make this handling of mulens better
    val joinColumns =
      parsed.columns.flatMap(f => if (f == "mulens") List("mulens_class_1", "mulens_class_2") else List(f))
    val joinCondition = parsed.condition
    // joinColumns.map(c => col(s"${c}1") <=> col(s"${c}2")).reduce(_ || _)

    def selectCols(num: Int): List[Column] = col("id") :: joinColumns.map(x => col(x).as(s"${x}$num"))

    val df1New = df.select(selectCols(1): _*)
    val df2Old = loadedDf.select(selectCols(2): _*)

    val joinedOld = df1New.joinWith(df2Old, joinCondition).withColumn("similarity", lit(0L))
    val encoder   = org.apache.spark.sql.Encoders.product[MakeEdge]

    @inline
    def computeSimilarity(df: DataFrame): DataFrame =
      joinColumns
        .foldLeft(df)((curr, acc) =>
          curr.withColumn(
            "similarity",
            when(SimilarityExpParser.colNameToCondition(acc, "_1.", "_2."), col("similarity") + 1)
              .otherwise(col("similarity"))
          )
        )

    @inline
    def makeEdge(df: DataFrame): Dataset[MakeEdge] =
      df.select(col(s"_1.id").as("src"), col("_2.id").as("dst"), col("similarity").cast("string").as("label"))
        .as(encoder)

    val computeSimOld      = computeSimilarity(joinedOld)
    val edgesToOldVertices = makeEdge(computeSimOld)

    // Edges for new vertices
    val df2New = df.select(selectCols(2): _*)

    val joinedNew          = df1New.joinWith(df2New, joinCondition).withColumn("similarity", lit(0L))
    val computeSimNew      = computeSimilarity(joinedNew)
    val edgesToNewVertices = makeEdge(computeSimNew)

    edgesToOldVertices
      .union(edgesToNewVertices)
      // Filter out any loop edges because of self join
      .filter(r => r.src != r.dst)
      .repartition(config.parallelism, col("src"))
  }
}

object SimilarityClassifer {}
