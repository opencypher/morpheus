package org.opencypher.spark.api.spark

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.opencypher.spark.impl.spark.SparkCypherSessionImpl

trait SparkCypherSession {
  def sparkSession: SparkSession

  def importDataFrame(df: DataFrame): SparkCypherRecords
}

object SparkCypherSession {
  def create(implicit sparkSession: SparkSession): SparkCypherSession =
    new SparkCypherSessionImpl(sparkSession)
}
