package org.opencypher.spark.impl.syntax

import org.opencypher.spark.api.classes.Cypher
import org.opencypher.spark.api.graph.{CypherGraph, CypherResult}
import org.opencypher.spark.api.value.CypherValue

import scala.language.implicitConversions

trait CypherSyntax {

  implicit def cypherEngineGraphSyntax[G <: CypherGraph, C <: Cypher { type Graph = G }](graph: G)(implicit engine: C)
  : CypherGraphOps[G, C] =
    new CypherGraphOps(graph)


  implicit def cypherEngineResultSyntax[R <: CypherResult](result: R) : CypherResultOps[R] = new CypherResultOps(result)
}

final class CypherResultOps[R <: CypherResult](val result: R) extends AnyVal {

  def cypher[C <: Cypher { type Graph = result.Graph; type Result = R }](query: String)(implicit engine: C): R =
    engine.cypher(result.graph, query)

  def cypher[C <: Cypher { type Graph = result.Graph; type Result = R }](
    query: String,
    parameters: Map[String, CypherValue]
  )(implicit engine: C): R =
    engine.cypher(result.graph, query)
}

final class CypherGraphOps[G <: CypherGraph, C <: Cypher { type Graph = G }](graph: G)(implicit val engine: C) {

  def cypher(query: String): engine.Result =
    engine.cypher(graph, query)

  def cypher(query: String, parameters: Map[String, CypherValue]): engine.Result =
    engine.cypher(graph, query, parameters)
}
