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
import com.astrolabsoftware.grafink.models.GrafinkException.MissingFixedVertex

case class Edge(src: Long, dst: Long, propVal: Double)

class TwoModeClassifier(config: TwoModeSimilarityConfig, similarityRecipes: List[FixedVertex])
    extends VertexClassifierRule {

  val scoreCond: Row => Boolean = r => (r.getAs[Double]("rfscore") > 0.9) && (r.getAs[Double]("snnscore") > 0.9)
  val roidCond: Row => Boolean  = r => r.getAs[Int]("roid") > 1
  val mulensmlCond: Row => Boolean = r =>
    (r.getAs[String]("mulens_class_1") == "ML") && (r.getAs[String]("mulens_class_2") == "ML")

  override def name: String = "similarityClassifier"

  override def getEdgeLabel: String = "satr"

  override def getEdgePropertyKey: String = "weight"

  override def classify(loadedDf: DataFrame, df: DataFrame): DataFrame = {

    val rules = config.recipes

    val ruleToCondition =
      Map(
        "supernova"    -> scoreCond,
        "microlensing" -> mulensmlCond,
        "asteroids"    -> roidCond
      )

    val directRules = rules.filter(ruleToCondition.contains)

    // Map from Rule to id of the similarity vertex
    val ruleToId = directRules.map { rule =>
      val recipe = similarityRecipes.filter(k => k.properties.exists(p => p.value.toString == rule))
      if (recipe.isEmpty) {
        throw MissingFixedVertex(s"No fixed vertex found in csv for configured rule $rule")
      }
      rule -> recipe.head.id
    }.toMap

    implicit val ec: Encoder[Edge] = Encoders.product[Edge]

    val edges =
      df.flatMap { row =>
        directRules.flatMap { rule =>
          // Normal rule based connections
          if (ruleToCondition(rule)(row)) Some(Edge(row.getAs[Long]("id"), ruleToId(rule), 0.0)) else None
        }
      }

    edges.toDF()
  }
}
