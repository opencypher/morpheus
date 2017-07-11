package org.opencypher.spark.api.graph

import org.opencypher.spark.api.record.CypherRecords

trait CypherResult {

  type Result <: CypherResult
  type Graph <: CypherGraph
  type Records <: CypherRecords

  def graph: Graph
  def records: Records

  def result(name: String): Option[Result]
  def namedGraph(name: String): Option[Graph]
}
