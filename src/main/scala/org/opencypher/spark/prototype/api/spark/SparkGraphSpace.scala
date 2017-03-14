package org.opencypher.spark.prototype.api.spark

import org.opencypher.spark.prototype.api.graph.GraphSpace
import org.opencypher.spark.prototype.impl.spark.SparkGraphLoading

trait SparkGraphSpace extends GraphSpace {
  override type Graph = SparkCypherGraph
}

object SparkGraphSpace extends SparkGraphLoading with Serializable
