package org.opencypher.spark.api.spark

import org.opencypher.spark.api.graph.CypherGraph

trait SparkCypherGraph extends CypherGraph {

  self =>

  override type Space = SparkGraphSpace
  override type Graph = SparkCypherGraph
  override type Records = SparkCypherRecords
}
