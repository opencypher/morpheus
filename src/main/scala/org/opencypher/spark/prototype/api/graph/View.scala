package org.opencypher.spark.prototype.api.graph

import org.opencypher.spark.prototype.api.expr.Expr
import org.opencypher.spark.prototype.api.value.CypherValue
import org.opencypher.spark.prototype.api.frame.Frame
import org.opencypher.spark.prototype.api.ir.QueryModel

trait View {
  def domain: Graph

  def graph: Graph
  def frame: Frame

  def parameters: Map[String, CypherValue]

  def model: QueryModel[Expr]
}
