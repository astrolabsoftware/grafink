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

import org.apache.spark.sql.{ DataFrame, Dataset }

trait VertexClassifierRule {

  def name: String

  /**
   * Given data already existing in graph (loadedDf) and the new data to ingest (df)
   * Return a DataFrame of Edge rows. In all the rows of the returned DataFrame, src id
   * must always be of the new data to ingest (df).
   * The contract mandates the returned DataFrame to have the requiredEdgeColumns as defined
   * in the EdgeProcessor.
   * @param df
   * @return DataFrame
   */
  def classify(loadedDf: DataFrame, df: DataFrame): DataFrame

  def getEdgeLabel: String

  def getEdgePropertyKey: String
}
