package org.opencypher.spark.prototype.api.graph

import org.opencypher.spark.prototype.api.expr.Expr
import org.opencypher.spark.prototype.api.value.CypherValue
import org.opencypher.spark.prototype.api.record.{CypherRecords, SparkCypherRecords}
import org.opencypher.spark.prototype.api.ir.QueryModel

trait CypherView {
  type Graph <: CypherGraph
  type Records <: CypherRecords

  def domain: Graph

  def graph: Graph
  def records: Records

  def parameters: Map[String, CypherValue]

  def model: QueryModel[Expr]
}

trait SparkCypherView extends CypherView {
  override type Graph = SparkCypherGraph
  override type Records = SparkCypherRecords
}
