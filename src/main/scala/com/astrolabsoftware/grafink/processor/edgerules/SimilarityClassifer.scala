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

/**
 * Logic to match `similar` alerts so that we can create edges between them in the graph
 * @param config
 */
class SimilarityClassifer(config: SimilarityConfig) extends VertexClassifierRule {

  override def name: String = "similarityClassifier"

  override def getEdgeLabel: String = "similarity"

  /**
   * Given a loaded data df (existing data in janusgraph), and current data df, return
   * a Dataset of MakeEdge after applying the logic
   *
   * @param df
   * @return
   */
  override def classify(loadedDf: DataFrame, df: DataFrame): Dataset[MakeEdge] = {

    val similarityExpression = config.similarityExp
    val parsed               = SimilarityExpParser.parse(similarityExpression)
    val joinColumns          = parsed.columns
    // The id greater than condition prevents duplicate rows as a result of cross join
    val joinCondition        = (col("id1") > col("id2")) && parsed.condition

    // TODO: Make this handling of mulens better
    val selectColsNoIdList: List[String] =
      joinColumns.flatMap(f => if (f == "mulens") List("mulens_class_1", "mulens_class_2") else List(f))

    val selectColsList: List[String] = "id" :: selectColsNoIdList

    @inline
    def selectCols: List[Column] = selectColsList.map(col)

    @inline
    def selectColsWithSuffix(num: Int, list: List[String]): List[Column] = list.map(x => col(x).as(s"${x}$num"))

    @inline
    def selectColsHavingSuffixAndPrefix(suffix: Int, prefix: String): List[Column] =
      selectColsList.map(x => col(s"$prefix$x$suffix").as(s"${x}$suffix"))

    val df1New = df.select(selectColsWithSuffix(1, selectColsList): _*)
    val df2Old =
      loadedDf
        .select(selectCols: _*)
        // Take union with new vertices since we want to create edges within them as well
        .union(df.select(selectCols: _*))
        .select(selectColsWithSuffix(2, selectColsList): _*)

    val joined =
      df1New
        .joinWith(df2Old, joinCondition)
        .select(
          selectColsHavingSuffixAndPrefix(1, "_1.") ++
            selectColsHavingSuffixAndPrefix(2, "_2."): _*
        )
        .withColumn("similarity", lit(0L))

    val encoder = org.apache.spark.sql.Encoders.product[MakeEdge]

    /**
     * Given the joined dataframe, adds 1 to the similarity column for each
     * matching join condition, thereby computing `similarity`
     * @param df
     * @return
     */
    @inline
    def computeSimilarity(df: DataFrame): DataFrame =
      joinColumns
        .foldLeft(df)((curr, acc) =>
          curr.withColumn(
            "similarity",
            when(SimilarityExpParser.colNameToCondition(acc), col("similarity") + 1)
              .otherwise(col("similarity"))
          )
        )

    @inline
    def makeEdge(df: DataFrame): Dataset[MakeEdge] =
      df.select(col(s"id1").as("src"), col("id2").as("dst"), col("similarity").as("propVal"))
        .as(encoder)

    val computeSim = computeSimilarity(joined)

    makeEdge(computeSim)
  }
}

object SimilarityClassifer {}
