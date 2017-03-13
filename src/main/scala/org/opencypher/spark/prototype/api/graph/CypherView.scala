package org.opencypher.spark.prototype.api.graph

import org.opencypher.spark.prototype.api.expr.Expr
import org.opencypher.spark.prototype.api.ir.QueryModel
import org.opencypher.spark.prototype.api.record.CypherRecords

trait CypherView {
  type Graph <: CypherGraph
  type Records <: CypherRecords

  def domain: Graph

  def graph: Graph
  def records: Records

  def model: QueryModel[Expr]
}


