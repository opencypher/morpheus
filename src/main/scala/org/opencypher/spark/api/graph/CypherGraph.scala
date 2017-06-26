package org.opencypher.spark.api.graph

import org.opencypher.spark.api.expr.{Expr, Var}
import org.opencypher.spark.api.ir.QueryModel
import org.opencypher.spark.api.record.CypherRecords
import org.opencypher.spark.api.schema.Schema

trait CypherGraph {

  self =>

  type Space <: GraphSpace { type Graph = self.Graph }
  type Graph <: CypherGraph { type Records = self.Records }
  type Records <: CypherRecords { type Records = self.Records }

  def space: Space

  def schema: Schema

  def records: Records = details.compact
  def details: Records

  // TODO: This opens up for someone to send a wrongly typed variable here -- name here, type on inside?
  def nodes(v: Var): Records
  def relationships(v: Var): Records

//  def filterNodes()
//  def filterRelationships()

//  def union(other: Graph): Graph
//  def intersect(other: Graph): Graph

  // TODO
  // other attributes, other views (constituents, triplets, domain)
}

