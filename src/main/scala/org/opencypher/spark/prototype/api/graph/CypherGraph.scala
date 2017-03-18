package org.opencypher.spark.prototype.api.graph

import org.opencypher.spark.prototype.api.expr.{Expr, Var}
import org.opencypher.spark.prototype.api.ir.QueryModel
import org.opencypher.spark.prototype.api.record.CypherRecords
import org.opencypher.spark.prototype.api.schema.Schema

trait CypherGraph {

  self =>

  type Space <: GraphSpace
  type Graph <: CypherGraph
  type Records <: CypherRecords { type Records = self.Records }

  def space: Space

  def model: QueryModel[Expr]
  def schema: Schema

  def records: Records = details.compact
  def details: Records

  def nodes(v: Var): Graph
  def relationships(v: Var): Graph

//  def filterNodes()
//  def filterRelationships()

//  def union(other: Graph): Graph
//  def intersect(other: Graph): Graph

  // TODO
  // other attributes, other views (constituents, triplets, domain)
}

