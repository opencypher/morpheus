/*
 * Copyright (c) 2016-2018 "Neo4j, Inc." [https://neo4j.com]
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

import org.opencypher.caps.api.io.{CreateOrFail, PropertyGraphDataSource, PersistMode}
import org.opencypher.caps.api.record.CypherRecords
import org.opencypher.caps.api.value.CypherValue

/**
  * The session that manages graphs and adds Cypher query capabilities to them.
  * Sessions serve as containers for configuration and provide graph loading via URIs.
  */
trait CypherSession {

  /**
    * An immutable empty graph.
    *
    * @return an immutable empty graph.
    */
  def emptyGraph: PropertyGraph

  final def cypher(query: String): CypherResult =
    cypher(emptyGraph, query, Map.empty)

  final def cypher(query: String, parameters: Map[String, CypherValue]): CypherResult =
    cypher(emptyGraph, query, parameters)

  final def cypher(graph: PropertyGraph, query: String): CypherResult =
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
  def cypher(graph: PropertyGraph, query: String, parameters: Map[String, CypherValue]): CypherResult

  /**
    * Retrieves the graph from the argument URI, if it exists.
    *
    * @param uri the uri locating the graph.
    * @return the graph located at the uri.
    */
  def graphAt(uri: URI): PropertyGraph

  def graphAt(uri: String): PropertyGraph =
    graphAt(URI.create(uri))

  /**
    * Mounts the given graph source to session-local storage under the given path. The graph will be accessible under
    * the session-local URI scheme, e.g. session://<path>.
    *
    * @param source the graph source to register.
    * @param path the path at which this graph source will be discoverable.
    */
  def mountSourceAt(source: PropertyGraphDataSource, path: String): Unit

  /**
    * Stores the given graph at the location and in the format given by the URI, with the overwrite semantics given by
    * the mode.
    *
    * @param graph the graph to store.
    * @param uri the graph URI indicating location and format to store the graph in.
    * @param mode the persist mode which determines what happens if the location is occupied.
    * @return the stored graph.
    */
  def storeGraphAt(graph: PropertyGraph, uri: String, mode: PersistMode = CreateOrFail): PropertyGraph
}

object CypherSession {
  val sessionGraphSchema = "session"
}
