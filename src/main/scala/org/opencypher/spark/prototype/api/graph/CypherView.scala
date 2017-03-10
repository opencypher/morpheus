package org.opencypher.spark.prototype.api.graph

import org.opencypher.spark.prototype.api.expr.Expr
import org.opencypher.spark.prototype.api.ir.QueryModel
import org.opencypher.spark.prototype.api.record.{CypherRecords, SparkCypherRecords}

trait CypherView {
  type Graph <: CypherGraph
  type Records <: CypherRecords

  def domain: Graph

  def graph: Graph
  def records: Records

  def model: QueryModel[Expr]

  def show(): Unit
}

trait SparkCypherView extends CypherView {
  override type Graph = SparkCypherGraph
  override type Records = SparkCypherRecords

  override def show() = records.toDF.show()
}
