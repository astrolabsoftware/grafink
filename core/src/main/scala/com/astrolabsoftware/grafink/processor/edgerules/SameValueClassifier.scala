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

import org.apache.spark.sql.{ DataFrame, Encoder, Encoders }
import org.apache.spark.sql.functions.{ col, lit }

import com.astrolabsoftware.grafink.models.SameValueSimilarityConfig
import com.astrolabsoftware.grafink.processor.EdgeProcessor.EdgeColumns.{
  DSTVERTEXFIELD,
  PROPERTYVALFIELD,
  SRCVERTEXFIELD
}

case class SameValueEdge(src: Long, dst: Long, propVal: String)

class SameValueClassifier(config: SameValueSimilarityConfig) extends VertexClassifierRule {

  override def name: String = "similarityClassifier"

  override def getEdgeLabel: String = "exactmatch"

  override def getEdgePropertyKey: String = "propertyname"

  override def classify(loadedDf: DataFrame, df: DataFrame): DataFrame = {
    import df.sparkSession.implicits._

    implicit val ec: Encoder[SameValueEdge] = Encoders.product[SameValueEdge]

    config.colsToConnect.map { column =>
      // Connect all vertices with same values of column
      val newVertices          = df.select(column, "id")
      val newVerticesToConnect = newVertices.groupByKey(r => r.getAs[String](column))

      val edgesFromNewVertices = newVerticesToConnect.flatMapGroups { (group, data) =>
        // Need to create edges from every vertex to every other vertex in this group
        data.map(r => r.getAs[Long]("id")).toList.combinations(2).map(l => SameValueEdge(l(0), l(1), column))
      }.toDF()

      newVertices
        .join(loadedDf.select(column, "id"), column)
        .select(
          df.col("id").as(SRCVERTEXFIELD),
          loadedDf.col("id").as(DSTVERTEXFIELD),
          lit(column).as(PROPERTYVALFIELD)
        )
        .union(edgesFromNewVertices)
    }.reduce(_ union _)
  }
}
