package org.opencypher.spark.prototype.api.spark

import org.apache.spark.sql.SparkSession
import org.opencypher.spark.prototype.api.graph.GraphSpace
import org.opencypher.spark.prototype.impl.spark.SparkGraphLoading

trait SparkGraphSpace extends GraphSpace {
  override type Graph = SparkCypherGraph

  def session: SparkSession
}

object SparkGraphSpace extends SparkGraphLoading with Serializable
