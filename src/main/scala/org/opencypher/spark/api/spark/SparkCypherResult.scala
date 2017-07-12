package org.opencypher.spark.api.spark

import org.opencypher.spark.api.graph.CypherResult

trait SparkCypherResult extends CypherResult {

  override type Result = SparkCypherResult
  override type Graph = SparkCypherGraph
  override type Records = SparkCypherRecords

  def recordsWithDetails = records.details

  override def namedGraph(name: String): Option[SparkCypherGraph] = result(name).map(_.graph)
}
