package org.opencypher.spark.prototype.impl.syntax

import org.opencypher.spark.prototype.api.graph.CypherGraph
import org.opencypher.spark.prototype.api.value.CypherValue
import org.opencypher.spark.prototype.impl.classy.Cypher

trait CypherSyntax {

  implicit def cypherEngineSyntax[G <: CypherGraph, C <: Cypher { type Graph = G }](graph: G)(implicit engine: C) =
    new CypherOps(graph)
}

final class CypherOps[G <: CypherGraph, C <: Cypher { type Graph = G }](graph: G)(implicit val engine: C) {

  def cypher(query: String): engine.View =
    engine.cypher(graph, query)

  def cypher(query: String, parameters: Map[String, CypherValue]): engine.View =
    engine.cypher(graph, query, parameters)
}
