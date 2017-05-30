package org.opencypher.spark.api.spark

import org.opencypher.spark.api.graph.CypherResult

trait SparkCypherResult extends CypherResult {

  override type Graph = SparkCypherGraph
  override type Records = SparkCypherRecords
  override type Result = SparkCypherResult


}
