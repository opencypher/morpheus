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

import org.opencypher.caps.api.io.{CreateOrFail, PersistMode, PropertyGraphDataSource}
import org.opencypher.caps.api.types._


trait PlaceholderCypherValue {
  def cypherType: CypherType
}

trait PlaceholderCypherNull extends PlaceholderCypherValue {
  override def cypherType = CTNull
}

trait PlaceholderCypherList extends PlaceholderCypherValue {
  override def cypherType: CTList
}

trait PlaceholderCypherBoolean extends PlaceholderCypherValue {
  override def cypherType = CTBoolean
}

trait PlaceholderCypherInteger extends PlaceholderCypherValue {
  def value: Long

  override def cypherType = CTInteger
}

trait PlaceholderCypherFloat extends PlaceholderCypherValue {
  def value: Double

  override def cypherType = CTFloat
}

trait PlaceholderCypherString extends PlaceholderCypherValue {
  def value: String

  override def cypherType = CTString
}

trait PlaceholderCypherMap extends PlaceholderCypherValue {
  def get(key: String): Option[PlaceholderCypherValue]

  def keys: Set[String]

  override def cypherType = CTMap
}

trait PlaceholderCypherPath extends PlaceholderCypherValue {

  override def cypherType = CTPath
}


// TODO: extend doc with explanation for writing graphs
/**
  * The Cypher Session is the main API for a Cypher-based application. It manages graphs which can be queried using
  * Cypher. Graphs can be read from different data sources (e.g. CSV) and mounted in the session-local storage.
  */
trait CypherSession {

  /**
    * Executes a Cypher query in this session on the current ambient graph.
    *
    * @param query      Cypher query to execute
    * @param parameters parameters used by the Cypher query
    * @return result of the query
    */
  def cypher(query: String, parameters: Map[String, PlaceholderCypherValue] = Map.empty): CypherResult

  /**
    * Executes a Cypher query in this session, using the argument graph as the ambient graph.
    *
    * The ambient graph is the graph that is used for graph matching and updating,
    * unless another graph is explicitly selected by the query.
    *
    * @param graph      ambient graph for this query
    * @param query      Cypher query to execute
    * @param parameters parameters used by the Cypher query
    * @return result of the query
    */
  private[graph] def cypherOnGraph(graph: PropertyGraph, query: String, parameters: Map[String, PlaceholderCypherValue] = Map.empty): CypherResult

  /**
    * Reads a graph from the argument URI.
    *
    * @param uri URI locating a graph
    * @return graph located at the URI
    */
  def readFrom(uri: URI): PropertyGraph

  /**
    * Reads a graph from an argument string that represents a valid URI.
    *
    * @param uri URI string locating a graph
    * @return graph located at the URI
    */
  def readFrom(uri: String): PropertyGraph = readFrom(URI.create(uri))

  /**
    * Mounts the given graph source to session-local storage under the given path. The specified graph will be
    * accessible under the session-local URI scheme, e.g. {{{session://$path}}}.
    *
    * @param source graph source to register
    * @param path   path at which this graph can be accessed via {{{session://$path}}}
    */
  def mount(source: PropertyGraphDataSource, path: String): Unit

  /**
    * Writes the given graph to the location using the format specified by the URI.
    *
    * @param graph graph to write
    * @param uri   graph URI indicating location and format to write the graph to
    * @param mode  persist mode which determines what happens if the location is occupied
    */
  // TODO: Error handling via Return Type (Success, Failed) or just throw exception?
  def write(graph: PropertyGraph, uri: String, mode: PersistMode = CreateOrFail): Unit
}

object CypherSession {
  val sessionGraphSchema = "session"
}
