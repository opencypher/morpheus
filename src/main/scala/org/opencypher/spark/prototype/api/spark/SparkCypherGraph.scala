package org.opencypher.spark.prototype.api.spark

import org.opencypher.spark.prototype.api.graph.CypherGraph

trait SparkCypherGraph extends CypherGraph {
  override type Space = SparkGraphSpace
  override type View = SparkCypherView
  override type Graph = SparkCypherGraph
}
