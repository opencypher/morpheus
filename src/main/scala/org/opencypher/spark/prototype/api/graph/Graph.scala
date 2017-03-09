package org.opencypher.spark.prototype.api.graph

import org.opencypher.spark.prototype.api.schema.Schema

trait Graph {
  def space: GraphSpace

  def nodes: View
  def relationships: View

  def views: Set[View]

  def schema: Schema

  // identity
  // properties
  // labels
}
