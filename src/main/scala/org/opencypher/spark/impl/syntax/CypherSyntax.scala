/**
 * Copyright (c) 2016-2017 "Neo4j, Inc." [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
