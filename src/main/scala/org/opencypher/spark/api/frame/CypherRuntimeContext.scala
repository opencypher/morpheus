package org.opencypher.spark.api.frame

import org.apache.spark.sql.SparkSession

trait CypherRuntimeContext {
  def session: SparkSession
}
