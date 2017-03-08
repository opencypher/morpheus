package org.opencypher.spark.prototype.graph

import org.opencypher.spark.api.value.CypherValue
import org.opencypher.spark.prototype.Expr
import org.opencypher.spark.prototype.ir.QueryModel

trait View {
  def domain: Graph

  def graph: Graph
  def frame: Unit

  def parameters: Map[String, CypherValue]

  def model: QueryModel[Expr]
}
