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

import org.apache.spark.sql.{ DataFrame, Encoder, Encoders, Row }

import com.astrolabsoftware.grafink.models.{ FixedVertex, TwoModeSimilarityConfig }

case class Edge(src: Long, dst: Long, propVal: Double)

class TwoModeClassifier(config: TwoModeSimilarityConfig, similarityRecipes: List[FixedVertex])
    extends VertexClassifierRule {

  val scoreCond: Row => Boolean = r => (r.getAs[Double]("rfscore") > 0.9) && (r.getAs[Double]("snnscore") > 0.9)
  val roidCond: Row => Boolean  = r => r.getAs[Int]("roid") > 1
  val mulensmlCond: Row => Boolean = r =>
    (r.getAs[String]("mulens1_class_1") == "ML") && (r.getAs[String]("mulens1_class_2") == "ML")

//  val scoreCond: (Column, Column) => Column = (score1, score2) => (score1 > 0.9) && (score2 > 0.9)
//  val roidCond: Column => Column = roid => roid > 1
//  val mulensmlCond: (Column, Column) => Column =
//    (mulens1_class_1, mulens1_class_2) =>
//      (mulens1_class_1 === "ML" && mulens1_class_2 === "ML")

  val ruleToCondition =
    Map(
      "supernova"    -> scoreCond,
      "microlensing" -> mulensmlCond,
      "asteroids"    -> roidCond
    )

  override def name: String = "similarityClassifier"

  override def getEdgeLabel: String = "satr"

  override def getEdgePropertyKey: String = "weight"

  override def classify(loadedDf: DataFrame, df: DataFrame): DataFrame = {

    val rules = config.recipes

    val directRules = rules.filter(ruleToCondition.contains)

    // Map from Rule to id of the similarity vertex
    val ruleToId = directRules.map { rule =>
      val recipe = similarityRecipes.filter(k => k.properties.exists(p => p.name == rule)).head
      rule -> recipe.id
    }.toMap

    implicit val ec: Encoder[Edge] = Encoders.product[Edge]

    val edges =
      df.flatMap { row =>
        rules.flatMap { rule =>
          // Normal rule based connections
          if (ruleToCondition(rule)(row)) Some(Edge(row.getAs[Long]("id"), ruleToId(rule), 0.0)) else None
        }
      }

    edges.toDF()
  }
}
