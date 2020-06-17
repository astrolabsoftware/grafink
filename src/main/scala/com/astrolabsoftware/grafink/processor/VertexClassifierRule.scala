package com.astrolabsoftware.grafink.processor

import org.apache.spark.sql.{DataFrame, Dataset}

import com.astrolabsoftware.grafink.processor.EdgeProcessor.MakeEdge

trait VertexClassifierRule {

  def name: String

  /**
   * Given data already existing in graph (loadedDf) and the new data to ingest (df)
   * Return a Dataset of {@link MakeEdge}. In all the rows of the returned RDD, src id
   * must always be of the new data to ingest (df).
   * @param df
   * @return DataFrame
   */
  def classify(loadedDf: DataFrame, df: DataFrame): Dataset[MakeEdge]
}
