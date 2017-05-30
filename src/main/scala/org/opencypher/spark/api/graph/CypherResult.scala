package org.opencypher.spark.api.graph

import org.opencypher.spark.api.record.CypherRecords

trait CypherResult {

  type Graph <: CypherGraph
  type Records <: CypherRecords
  type Result <: CypherResult

  def graph: Graph
  def records: Records

  def result(name: String): Option[Result]
}
