package org.opencypher.spark.prototype.api.graph

import org.opencypher.spark.prototype.api.ir.global.GlobalsRegistry

// (4) Figure out physical plan
// (5) Execute and flesh out user facing api
trait GraphSpace {
  type Graph <: CypherGraph

  def base: Graph
  def globals: GlobalsRegistry

//  def graphs: Set[Graph]
}


