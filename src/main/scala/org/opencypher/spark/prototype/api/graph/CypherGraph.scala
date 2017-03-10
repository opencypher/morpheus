package org.opencypher.spark.prototype.api.graph

import org.opencypher.spark.prototype.api.schema.Schema

trait CypherGraph {
  type Space <: GraphSpace
  type View <: CypherView

  def space: Space

  def nodes: View
  def relationships: View

  def constituents: Set[View]

  def schema: Schema

  // identity
  // properties
  // labels
}

trait SparkCypherGraph extends CypherGraph {
  override type Space = SparkGraphSpace
  override type View = SparkCypherView
}
