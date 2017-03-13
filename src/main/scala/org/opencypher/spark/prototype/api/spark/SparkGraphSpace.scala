package org.opencypher.spark.prototype.api.spark

import org.opencypher.spark.prototype.api.graph.GraphSpace
import org.opencypher.spark.prototype.impl.spark.SparkGraphSpaceLoading

trait SparkGraphSpace extends GraphSpace {
  override type Graph = SparkCypherGraph
}

object SparkGraphSpace extends SparkGraphSpaceLoading with Serializable
