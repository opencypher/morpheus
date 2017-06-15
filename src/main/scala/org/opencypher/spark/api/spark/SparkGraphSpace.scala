package org.opencypher.spark.api.spark

import org.apache.spark.sql.SparkSession
import org.opencypher.spark.api.graph.GraphSpace
import org.opencypher.spark.impl.spark.SparkGraphLoading

trait SparkGraphSpace extends GraphSpace {

  override type Space = SparkGraphSpace
  override type Graph = SparkCypherGraph
  override type Records = SparkCypherRecords

  // TODO: Remove
  def tokens: SparkCypherTokens

  def session: SparkSession
}

object SparkGraphSpace extends SparkGraphLoading with Serializable
