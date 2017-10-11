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
package org.opencypher.caps.api.io

import java.net.URI

import org.opencypher.caps.api.graph.{CypherGraph, CypherSession}
import org.opencypher.caps.api.schema.Schema

/**
  * Describes a location for a Cypher graph.
  */
trait GraphSource {

  self =>

  type Session <: CypherSession { type Session = self.Session; type Graph = self.Graph }
  type Graph <: CypherGraph { type Session = self.Session; type Graph = self.Graph }

  /**
    * The session tied to this graph source.
    */
  val session: Session

  /**
    * Determines whether this is a source for the graph at the argument URI.
    *
    * @param uri the location for a potential graph.
    * @return true if the graph at this graph source is located at the argument URI, otherwise false.
    */
  def sourceForGraphAt(uri: URI): Boolean

  /**
    * A canonical URI describing the location of this graph source.
    * The sourceForGraphAt function is guaranteed to return true for this URI.
    *
    * @return a URI describing the location of this graph source.
    */
  def canonicalURI: URI

  /**
    * Create a new empty graph stored in this graph source.
    *
    * @return the graph stored in this graph source.
    * @throws java.lang.RuntimeException if the graph could not be created or there already was a graph.
    */
  def create: Graph

  /**
    * Provides the graph stored in this graph source.
    *
    * @return the graph stored in this graph source.
    * @throws java.lang.RuntimeException if loading the graph could not be done.
    */
  def graph: Graph

  /**
    * Provides only the schema of the graph stored in this graph source or returns None if the schema cannot be
    * provided without loading/constructing the whole graph.
    *
    * @return the schema of the graph stored in this graph source.
    */
  def schema: Option[Schema] = None

  /**
    * Persists the argument graph to this source.
    *
    * @param graph the graph to persist.
    * @param mode  the persist mode to use.
    * @return the persisted graph.
    */
  def persist(graph: Graph, mode: PersistMode): Graph

  /**
    * Delete the graph stored at this graph source.
    * If no graph was located at this source, this operation is a no-op.
    */
  def delete(): Unit
}
