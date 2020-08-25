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

case class TwoModeEdge(src: Long, dst: Long, propVal: Double)

class TwoModeClassifier(config: TwoModeSimilarityConfig, similarityRecipes: List[FixedVertex])
    extends VertexClassifierRule {

  // Old supernova recipe
  val scoreCond: Row => Boolean = r =>
    (r.getAs[Double]("rfscore") > 0.9) && (r.getAs[Double]("snn_snia_vs_nonia") > 0.9)
  val roidCond: Row => Boolean = r => r.getAs[Int]("roid") > 1
  val mulensmlCond: Row => Boolean = r =>
    (r.getAs[String]("mulens_class_1") == "ML") && (r.getAs[String]("mulens_class_2") == "ML")

  override def name: String = "similarityClassifier"

  override def getEdgeLabel: String = "satr"

  override def getEdgePropertyKey: String = "weight"

  override def classify(loadedDf: DataFrame, df: DataFrame): DataFrame = {

    val rules = config.recipes

    val supernovaRecipeCdsxmatchSet =
      Set(
        "galaxy",
        "Galaxy",
        "EmG",
        "Seyfert",
        "Seyfert_1",
        "Seyfert_2",
        "BlueCompG",
        "StarburstG",
        "LSB_G",
        "HII_G",
        "High_z_G",
        "GinPair",
        "GinGroup",
        "BClG",
        "GinCl",
        "PartofG",
        "Unknown",
        "Candidate_SN*",
        "SN",
        "Transient"
      )

    // New supernova recipe
    val supernovaCond: Row => Boolean = r =>
      (r.getAs[Double]("snn_snia_vs_nonia") > 0.75) &&
        (r.getAs[Float]("snn_sn_vs_all") > 0.75f) &&
        (r.getAs[Float]("drb") > 0.5) &&
        (r.getAs[Int]("ndethist") < 400) &&
        (r.getAs[Double]("classtar") > 0.4) &&
        (supernovaRecipeCdsxmatchSet.contains(r.getAs[String]("cdsxmatch")))

    val ruleToCondition =
      Map(
        "supernova"    -> supernovaCond,
        "microlensing" -> mulensmlCond,
        "asteroids"    -> roidCond
      )

    val ruleToColumnName =
      Map("catalog" -> "cdsxmatch")

    val directRules     = rules.filter(ruleToCondition.contains)
    val exactMatchRules = rules.filterNot(ruleToCondition.contains)

    val getRecipesForRule: String => List[FixedVertex] = rule =>
      similarityRecipes.filter(k => k.properties.exists(p => p.value.toString == rule))

    // Map from Rule to id of the similarity vertex
    val ruleToId = directRules.map { rule =>
      val recipe = getRecipesForRule(rule)
      if (recipe.isEmpty) {
        throw MissingFixedVertex(s"No fixed vertex found in csv for configured rule $rule")
      }
      rule -> recipe.head.id
    }.toMap

    implicit val ec: Encoder[TwoModeEdge] = Encoders.product[TwoModeEdge]

    val edges =
      df.flatMap { row =>
        directRules.flatMap { rule =>
          // Normal rule based connections
          if (ruleToCondition(rule)(row)) Some(TwoModeEdge(row.getAs[Long]("id"), ruleToId(rule), 0.0)) else None
        }
      }

    // Handles catalog
    val exactMatchRuleToId: Map[String, Map[String, Long]] = exactMatchRules.map { rule =>
      val recipe = getRecipesForRule(rule)
      if (recipe.isEmpty) {
        throw MissingFixedVertex(s"No fixed vertex found in csv for configured rule $rule")
      }
      rule -> recipe.map { r =>
        val vProp = r.properties.find(p => p.name == "equals")
        if (vProp.isEmpty) {
          throw new IllegalArgumentException(
            s"entry $r must have property equals since it is exact match recipe (rule $rule)"
          )
        }
        vProp.get.value.toString -> r.id
      }.toMap
    }.toMap

    val exactMatchEdges =
      df.flatMap { row =>
        exactMatchRules.flatMap { rule =>
          val colName = ruleToColumnName(rule)
          val v       = row.getAs[String](colName)
          if (exactMatchRuleToId(rule).contains(v)) {
            val targetId = exactMatchRuleToId(rule)(v)
            Some(TwoModeEdge(row.getAs[Long]("id"), targetId, 0.0))
          } else None
        }
      }

    edges.union(exactMatchEdges).toDF()
  }
}
