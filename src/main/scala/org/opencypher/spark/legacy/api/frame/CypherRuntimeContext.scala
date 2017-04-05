package org.opencypher.spark.legacy.api.frame

import org.apache.spark.sql.SparkSession

trait CypherRuntimeContext {
  def session: SparkSession
}
