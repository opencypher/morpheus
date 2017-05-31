package org.opencypher.spark.api.graph

import org.opencypher.spark.api.expr.Var
import org.opencypher.spark.api.record.CypherRecords
import org.opencypher.spark.api.schema.Schema
import org.opencypher.spark.api.types.{CTNode, CTRelationship}

trait CypherGraph {

  self =>

  type Space <: GraphSpace
  type Graph <: CypherGraph
  type Records <: CypherRecords { type Records = self.Records }

  def space: Space

  def schema: Schema

  // TODO: This opens up for someone to send a wrongly typed variable here -- name here, type on inside?
  def nodes(v: Var): Records
  def relationships(v: Var): Records

  def _nodes(name: String, typ: CTNode): Records = ???
  def _relationships(name: String, typ: CTRelationship): Records = ???

//  def filterNodes()
//  def filterRelationships()

//  def union(other: Graph): Graph
//  def intersect(other: Graph): Graph

  // TODO
  // other attributes, other views (constituents, triplets, domain)
}

