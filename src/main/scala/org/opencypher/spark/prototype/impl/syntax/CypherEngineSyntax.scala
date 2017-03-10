package org.opencypher.spark.prototype.impl.syntax

import org.opencypher.spark.prototype.api.graph.{CypherGraph, CypherView}
import org.opencypher.spark.prototype.api.value.CypherValue
import org.opencypher.spark.prototype.impl.classy.CypherEngine

trait CypherEngineSyntax {

  implicit def cypherEngineSyntax[G <: CypherGraph, V <: CypherView](graph: G)
  ( implicit
    engine: CypherEngine { type EGraph = G; type EView = V }
  ) =
    new CypherEngineOps(graph)
}

final class CypherEngineOps[G <: CypherGraph, V <: CypherView](graph: G)
  (implicit engine: CypherEngine { type EGraph = G; type EView = V }) {

  def cypher(query: String): V =
    engine.cypher(graph, query)
  def cypher(query: String, parameters: Map[String, CypherValue]): V =
    engine.cypher(graph, query, parameters)
}
