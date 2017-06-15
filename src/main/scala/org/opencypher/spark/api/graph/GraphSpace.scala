package org.opencypher.spark.api.graph

import org.opencypher.spark.api.ir.global.GlobalsRegistry

trait GraphSpace {
  type Graph <: CypherGraph

  def base: Graph
  def globals: GlobalsRegistry

  def graph(name: String): Option[Graph] = ???
}


