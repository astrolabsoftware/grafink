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
package org.apache.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.functions.lit

object SparkExtensions {

  /**
   * Optimized Spark SQL equivalent of RDD.zipWithIndex.
   * Avoids the serialization / deserialization from/to df's InternalRow's
   * underlying bytes array <--> GenericRow's underlying JVM objects collection Array[Any])
   *
   * @param df
   * @param offset Index offset from where to start assigning ids
   * @param indexColName
   * @return `df` with a column named `indexColName` of consecutive unique ids.
   */
  def zipWithIndex(df: DataFrame, offset: Long, indexColName: String = "id"): DataFrame = {
    import df.sparkSession.implicits._

    val dfWithIndexCol: DataFrame = df
      .drop(indexColName)
      .select(lit(0L).as(indexColName), $"*")

    val internalRows: RDD[InternalRow] = dfWithIndexCol.queryExecution.toRdd
      .zipWithIndex()
      .map {
        case (internalRow: InternalRow, index: Long) =>
          internalRow.setLong(0, offset + index)
          internalRow
      }

    Dataset.ofRows(
      df.sparkSession,
      LogicalRDD(dfWithIndexCol.schema.toAttributes, internalRows)(df.sparkSession)
    )
  }
}
