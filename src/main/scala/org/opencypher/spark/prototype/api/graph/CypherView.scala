package org.opencypher.spark.prototype.api.graph

import org.opencypher.spark.prototype.api.expr.Expr
import org.opencypher.spark.prototype.api.value.CypherValue
import org.opencypher.spark.prototype.api.record.CypherRecords
import org.opencypher.spark.prototype.api.ir.QueryModel

trait CypherView {
  def domain: CypherGraph

  def graph: CypherGraph
  def records: CypherRecords

  def parameters: Map[String, CypherValue]

  def model: QueryModel[Expr]
}
