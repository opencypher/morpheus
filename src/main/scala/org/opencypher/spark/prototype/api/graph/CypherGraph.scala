package org.opencypher.spark.prototype.api.graph

import org.opencypher.spark.prototype.api.schema.Schema

trait CypherGraph {
  def space: GraphSpace

  def nodes: CypherView
  def relationships: CypherView

  def views: Set[CypherView]

  def schema: Schema

  // identity
  // properties
  // labels
}
