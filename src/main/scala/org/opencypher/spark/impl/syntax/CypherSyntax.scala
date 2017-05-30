package org.opencypher.spark.impl.syntax

import org.opencypher.spark.api.graph.{CypherGraph, CypherResult}
import org.opencypher.spark.api.spark.{SparkCypherGraph, SparkCypherResult}
import org.opencypher.spark.api.value.CypherValue
import org.opencypher.spark.impl.classes.Cypher

import scala.language.implicitConversions

trait CypherSyntax {

  implicit def cypherEngineGraphSyntax[G <: CypherGraph, C <: Cypher { type Graph = G }](graph: G)(implicit engine: C)
  : CypherGraphOps[G, C] =
    new CypherGraphOps(graph)
}

final class CypherGraphOps[G <: CypherGraph, C <: Cypher { type Graph = G }](graph: G)(implicit val engine: C) {

  def cypher(query: String): engine.Result =
    engine.cypher(graph, query)

  def cypher(query: String, parameters: Map[String, CypherValue]): engine.Result =
    engine.cypher(graph, query, parameters)
}
