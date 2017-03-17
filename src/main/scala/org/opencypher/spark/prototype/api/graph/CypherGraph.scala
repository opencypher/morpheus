package org.opencypher.spark.prototype.api.graph

import org.opencypher.spark.prototype.api.expr.{Expr, Var}
import org.opencypher.spark.prototype.api.ir.QueryModel
import org.opencypher.spark.prototype.api.record.CypherRecords
import org.opencypher.spark.prototype.api.schema.Schema

trait CypherGraph {
  type Space <: GraphSpace
  type Graph <: CypherGraph
  type Records <: CypherRecords

  def space: Space
  def domain: Graph

  def model: QueryModel[Expr]
  def schema: Schema

  def records: Records

  def nodes(v: Var): Graph
  def relationships(v: Var): Graph
  // def triplets(start: Var, rel: Var, end: Var): Records

  //  def constituents: Set[Graph]

  // identity
  // properties
  // labels
}

