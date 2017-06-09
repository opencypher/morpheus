package org.opencypher.spark.api.spark

import org.apache.spark.sql.SparkSession
import org.opencypher.spark.impl.spark.SparkCypherSessionImpl

trait SparkCypherSession {
  def sparkSession: SparkSession
}

object SparkCypherSession {
  def create(implicit sparkSession: SparkSession): SparkCypherSession =
    new SparkCypherSessionImpl(sparkSession)
}
