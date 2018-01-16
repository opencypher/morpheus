/*
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
package org.opencypher.caps.api.graph

import java.net.URI

import org.opencypher.caps.api.record.CypherRecords
import org.opencypher.caps.api.value.CypherValue

/**
  * The session that manages graphs and adds Cypher query capabilities to them.
  * Sessions serve as containers for configuration and provide graph loading via URIs.
  */
trait CypherSession {

  self =>

  type Graph <: CypherGraph { type Graph = self.Graph; type Records = self.Records }
  type Session <: CypherSession {
    type Session = self.Session; type Graph = self.Graph; type Records = self.Records; type Result = self.Result;
    type Data = self.Data
  }
  type Records <: CypherRecords { type Records = self.Records; type Data = self.Data }
  type Result <: CypherResult { type Graph = self.Graph; type Records = self.Records }
  type Data

  /**
    * An immutable empty graph.
    *
    * @return an immutable empty graph.
    */
  def emptyGraph: Graph

  final def cypher(query: String): Result =
    cypher(emptyGraph, query, Map.empty)

  final def cypher(query: String, parameters: Map[String, CypherValue]): Result =
    cypher(emptyGraph, query, parameters)

  final def cypher(graph: Graph, query: String): Result =
    cypher(graph, query, Map.empty)

  /**
    * Executes a Cypher query in this session, using the argument graph as the ambient graph.
    *
    * The ambient graph is the graph that is used for graph matching and updating,
    * unless another graph is explicitly selected by the query.
    *
    * @param graph      the ambient graph for this query.
    * @param query      the Cypher query to execute.
    * @param parameters the parameters used by the Cypher query.
    * @return the result of the query.
    */
  def cypher(graph: Graph, query: String, parameters: Map[String, CypherValue]): Result

  /**
    * Retrieves the graph from the argument URI, if it exists.
    *
    * @param uri the uri locating the graph.
    * @return the graph located at the uri.
    */
  def graphAt(uri: URI): Graph
}

object CypherSession {
  val sessionGraphSchema = "session"
}
