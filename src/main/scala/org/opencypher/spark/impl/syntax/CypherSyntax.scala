package org.opencypher.spark.impl.syntax

import org.opencypher.spark.api.graph.CypherGraph
import org.opencypher.spark.api.value.CypherValue
import org.opencypher.spark.impl.classes.Cypher

import scala.language.implicitConversions

trait CypherSyntax {

  implicit def cypherEngineSyntax[G <: CypherGraph, C <: Cypher { type Graph = G }](graph: G)(implicit engine: C) =
    new CypherOps(graph)
}

final class CypherOps[G <: CypherGraph, C <: Cypher { type Graph = G }](graph: G)(implicit val engine: C) {

  def cypher(query: String): G =
    engine.cypher(graph, query)

  def cypher(query: String, parameters: Map[String, CypherValue]): G =
    engine.cypher(graph, query, parameters)
}
